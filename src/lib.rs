use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
};

use primitive::indexed_queue::{IndexedQueue, QueueIndex};

pub trait OrderKey: Clone + Eq + core::hash::Hash {}

#[derive(Debug, Clone)]
pub struct LimitOrder<K> {
    pub key: K,
    pub direction: Direction,
    pub price: UnitPrice,
    pub quantity: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnitPrice(NonZeroUsize);
impl UnitPrice {
    pub fn new(value: NonZeroUsize) -> Self {
        Self(value)
    }
    pub fn get(self) -> NonZeroUsize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Direction {
    Buy,
    Sell,
}
impl Direction {
    pub fn flip(&self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filled<K> {
    pub key: K,
    pub quantity: NonZeroUsize,
}

#[derive(Debug, Clone)]
pub struct AutoMatcher<K> {
    both_queues: BothDirectionData<BTreeMap<UnitPrice, PriceQueue<K>>>,
    orders: HashMap<K, (UnitPrice, Direction, QueueIndex)>,
    reused_queues: Vec<PriceQueue<K>>,
}
impl<K> AutoMatcher<K> {
    pub fn new() -> Self {
        Self {
            both_queues: BothDirectionData::new(),
            orders: HashMap::new(),
            reused_queues: vec![],
        }
    }
}
impl<K: OrderKey> AutoMatcher<K> {
    pub fn cancel_order(&mut self, key: &K) {
        let Some((price, direction, index)) = self.orders.remove(key) else {
            return;
        };
        let queues = self.both_queues.get_mut(direction);
        let queue = queues.get_mut(&price).unwrap();
        queue.cancel(index);
    }

    /// # Panic
    ///
    /// Panic if two orders with the same key are placed.
    pub fn place_order(
        &mut self,
        order: LimitOrder<K>,
        on_each_filled: &mut impl FnMut(Filled<K>),
    ) {
        assert!(!self.orders.contains_key(&order.key));
        let mut remaining_quantity = order.quantity;
        loop {
            let opp_queues = self.both_queues.get_mut(order.direction.flip());
            let best_matchable_queue = match order.direction {
                Direction::Buy => opp_queues
                    .first_entry()
                    .filter(|entry| *entry.key() <= order.price),
                Direction::Sell => opp_queues
                    .last_entry()
                    .filter(|entry| order.price <= *entry.key()),
            };
            let Some(mut best_matchable_queue) = best_matchable_queue else {
                break;
            };
            let queue = best_matchable_queue.get_mut();
            while let Some((filled, completion)) = queue.match_(remaining_quantity) {
                match completion {
                    OrderCompletion::Completed => {
                        self.orders.remove(&filled.key);
                    }
                    OrderCompletion::Open => (),
                }
                let remaining = remaining_quantity.get() - filled.quantity.get();
                on_each_filled(filled);
                let Some(remaining) = NonZeroUsize::new(remaining) else {
                    if queue.is_empty() {
                        self.reused_queues.push(best_matchable_queue.remove());
                    }
                    return;
                };
                remaining_quantity = remaining;
            }
            assert!(queue.is_empty());
            self.reused_queues.push(best_matchable_queue.remove());
        }
        let order = LimitOrder {
            quantity: remaining_quantity,
            ..order
        };
        self.insert_order(order);
    }

    fn insert_order(&mut self, order: LimitOrder<K>) {
        let queues = self.both_queues.get_mut(order.direction);
        let queue = queues.entry(order.price).or_insert_with(|| {
            if let Some(queue) = self.reused_queues.pop() {
                return queue;
            }
            PriceQueue::new()
        });
        let key = order.key.clone();
        let index = queue.push(order.key, order.quantity);
        self.orders
            .insert(key, (order.price, order.direction, index));
    }
}
impl<K> Default for AutoMatcher<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct BothDirectionData<T> {
    sell: T,
    buy: T,
}
impl<T: Default> BothDirectionData<T> {
    pub fn new() -> Self {
        Self {
            sell: T::default(),
            buy: T::default(),
        }
    }

    pub fn get_mut(&mut self, direction: Direction) -> &mut T {
        match direction {
            Direction::Buy => &mut self.buy,
            Direction::Sell => &mut self.sell,
        }
    }
}

#[derive(Debug, Clone)]
struct PriceQueue<K> {
    orders: IndexedQueue<OpenOrder<K>>,
}
impl<K> PriceQueue<K> {
    pub fn new() -> Self {
        Self {
            orders: IndexedQueue::new(),
        }
    }
}
impl<K: OrderKey> PriceQueue<K> {
    pub fn push(&mut self, key: K, quantity: NonZeroUsize) -> QueueIndex {
        self.orders.enqueue(OpenOrder::new(key, quantity))
    }

    pub fn cancel(&mut self, index: QueueIndex) {
        self.orders.remove(index);
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn match_(&mut self, quantity: NonZeroUsize) -> Option<(Filled<K>, OrderCompletion)> {
        let front = self.orders.front_mut()?;
        let (_, neural, front_quantity) = neutralize_quantity(quantity.get(), front.quantity.get());
        let filled = Filled {
            key: front.key.clone(),
            quantity: NonZeroUsize::new(neural).unwrap(),
        };
        let completion = match NonZeroUsize::new(front_quantity) {
            Some(front_quantity) => {
                front.quantity = front_quantity;
                OrderCompletion::Open
            }
            None => {
                self.orders.dequeue();
                OrderCompletion::Completed
            }
        };
        Some((filled, completion))
    }
}

#[derive(Debug, Clone)]
enum OrderCompletion {
    Completed,
    Open,
}

#[derive(Debug, Clone)]
struct OpenOrder<K> {
    pub key: K,
    pub quantity: NonZeroUsize,
}
impl<K> OpenOrder<K> {
    pub fn new(key: K, quantity: NonZeroUsize) -> Self {
        Self { key, quantity }
    }
}

fn neutralize_quantity(a: usize, b: usize) -> (usize, usize, usize) {
    let min = a.min(b);
    (a - min, min, b - min)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct MyOrderKey(usize);
    impl OrderKey for MyOrderKey {}

    #[test]
    fn test_place_cancel() {
        let mut matcher = AutoMatcher::new();
        let mut filled_buf = vec![];
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(0),
                    direction: Direction::Buy,
                    price: UnitPrice::new(NonZeroUsize::new(2).unwrap()),
                    quantity: NonZeroUsize::new(2).unwrap(),
                },
                &mut on_each_filled,
            );
            assert!(filled_buf.is_empty());
        }
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(1),
                    direction: Direction::Buy,
                    price: UnitPrice::new(NonZeroUsize::new(2).unwrap()),
                    quantity: NonZeroUsize::new(2).unwrap(),
                },
                &mut on_each_filled,
            );
            assert!(filled_buf.is_empty());
        }
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(2),
                    direction: Direction::Buy,
                    price: UnitPrice::new(NonZeroUsize::new(2).unwrap()),
                    quantity: NonZeroUsize::new(2).unwrap(),
                },
                &mut on_each_filled,
            );
            assert!(filled_buf.is_empty());
            matcher.cancel_order(&MyOrderKey(2));
        }
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(3),
                    direction: Direction::Buy,
                    price: UnitPrice::new(NonZeroUsize::new(1).unwrap()),
                    quantity: NonZeroUsize::new(2).unwrap(),
                },
                &mut on_each_filled,
            );
            assert!(filled_buf.is_empty());
        }
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(4),
                    direction: Direction::Sell,
                    price: UnitPrice::new(NonZeroUsize::new(1).unwrap()),
                    quantity: NonZeroUsize::new(3).unwrap(),
                },
                &mut on_each_filled,
            );
            assert_eq!(
                filled_buf,
                [
                    Filled {
                        key: MyOrderKey(0),
                        quantity: NonZeroUsize::new(2).unwrap()
                    },
                    Filled {
                        key: MyOrderKey(1),
                        quantity: NonZeroUsize::new(1).unwrap()
                    }
                ]
            );
            filled_buf.clear();
        }
        {
            let mut on_each_filled = |filled| {
                filled_buf.push(filled);
            };
            matcher.place_order(
                LimitOrder {
                    key: MyOrderKey(5),
                    direction: Direction::Sell,
                    price: UnitPrice::new(NonZeroUsize::new(1).unwrap()),
                    quantity: NonZeroUsize::new(3).unwrap(),
                },
                &mut on_each_filled,
            );
            assert_eq!(
                filled_buf,
                [
                    Filled {
                        key: MyOrderKey(1),
                        quantity: NonZeroUsize::new(1).unwrap()
                    },
                    Filled {
                        key: MyOrderKey(3),
                        quantity: NonZeroUsize::new(2).unwrap()
                    }
                ]
            );
            filled_buf.clear();
        }
    }
}
