use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
};

use primitive::{
    queue::indexed_queue::{IndexedQueue, QueueIndex},
    LenExt,
};

pub trait OrderKey: Clone + Eq + core::hash::Hash {}

#[derive(Debug, Clone)]
pub struct LimitOrder<K> {
    pub key: K,
    pub side: Side,
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
pub enum Side {
    Buy,
    Sell,
}
impl Side {
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
    pub completion: Completion,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Full,
    Partial,
}

#[derive(Debug, Clone)]
pub struct AutoMatcher<K> {
    both_queues: BothSideData<BTreeMap<UnitPrice, PriceQueue<K>>>,
    order_index: HashMap<K, (Side, UnitPrice, QueueIndex)>,
    reused_queues: Vec<PriceQueue<K>>,
}
impl<K> AutoMatcher<K> {
    pub fn new() -> Self {
        Self {
            both_queues: BothSideData::new(),
            order_index: HashMap::new(),
            reused_queues: vec![],
        }
    }
}
impl<K: OrderKey> AutoMatcher<K> {
    pub fn cancel_order(&mut self, key: &K) {
        let Some((side, price, index)) = self.order_index.remove(key) else {
            return;
        };
        let queues = self.both_queues.get_mut(side);
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
        assert!(!self.order_index.contains_key(&order.key));
        let mut remaining_quantity = order.quantity;
        loop {
            let opp_queues = self.both_queues.get_mut(order.side.flip());
            let best_matchable_queue = match order.side {
                Side::Buy => opp_queues
                    .first_entry()
                    .filter(|entry| *entry.key() <= order.price),
                Side::Sell => opp_queues
                    .last_entry()
                    .filter(|entry| order.price <= *entry.key()),
            };
            let Some(mut best_matchable_queue) = best_matchable_queue else {
                break;
            };
            let queue = best_matchable_queue.get_mut();
            while let Some(filled) = queue.match_(remaining_quantity) {
                match filled.completion {
                    Completion::Full => {
                        self.order_index.remove(&filled.key);
                    }
                    Completion::Partial => (),
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
        let queues = self.both_queues.get_mut(order.side);
        let queue = queues.entry(order.price).or_insert_with(|| {
            if let Some(queue) = self.reused_queues.pop() {
                return queue;
            }
            PriceQueue::new()
        });
        let key = order.key.clone();
        let index = queue.push(order.key, order.quantity);
        self.order_index
            .insert(key, (order.side, order.price, index));
    }
}
impl<K> Default for AutoMatcher<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct BothSideData<T> {
    sell: T,
    buy: T,
}
impl<T: Default> BothSideData<T> {
    pub fn new() -> Self {
        Self {
            sell: T::default(),
            buy: T::default(),
        }
    }

    pub fn get_mut(&mut self, side: Side) -> &mut T {
        match side {
            Side::Buy => &mut self.buy,
            Side::Sell => &mut self.sell,
        }
    }
}

#[derive(Debug, Clone)]
struct PriceQueue<K> {
    orders: IndexedQueue<UnfilledOrder<K>>,
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
        self.orders.enqueue(UnfilledOrder::new(key, quantity))
    }

    pub fn cancel(&mut self, index: QueueIndex) {
        self.orders.remove(index);
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn match_(&mut self, quantity: NonZeroUsize) -> Option<Filled<K>> {
        let front = self.orders.front_mut()?;
        let (_, neural, front_quantity) = neutralize_quantity(quantity.get(), front.quantity.get());
        let key = front.key.clone();
        let completion = match NonZeroUsize::new(front_quantity) {
            Some(front_quantity) => {
                front.quantity = front_quantity;
                Completion::Partial
            }
            None => {
                self.orders.dequeue();
                Completion::Full
            }
        };
        let filled = Filled {
            key,
            quantity: NonZeroUsize::new(neural).unwrap(),
            completion,
        };
        Some(filled)
    }
}

#[derive(Debug, Clone)]
struct UnfilledOrder<K> {
    pub key: K,
    pub quantity: NonZeroUsize,
}
impl<K> UnfilledOrder<K> {
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
                    side: Side::Buy,
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
                    side: Side::Buy,
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
                    side: Side::Buy,
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
                    side: Side::Buy,
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
                    side: Side::Sell,
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
                        quantity: NonZeroUsize::new(2).unwrap(),
                        completion: Completion::Full,
                    },
                    Filled {
                        key: MyOrderKey(1),
                        quantity: NonZeroUsize::new(1).unwrap(),
                        completion: Completion::Partial,
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
                    side: Side::Sell,
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
                        quantity: NonZeroUsize::new(1).unwrap(),
                        completion: Completion::Full,
                    },
                    Filled {
                        key: MyOrderKey(3),
                        quantity: NonZeroUsize::new(2).unwrap(),
                        completion: Completion::Full,
                    }
                ]
            );
            filled_buf.clear();
        }
    }
}
