use core::{cmp::Reverse, num::NonZeroUsize};

use primitive::{
    map::{linear_front_btree::LinearFrontBTreeMap11, MapInsert},
    ops::{clear::Clear, len::LenExt},
    queue::ind_queue::{IndQueue, QueueIndex},
};

#[derive(Debug, Clone)]
pub struct LimitOrder<K> {
    pub name: K,
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
    pub name: K,
    pub quantity: NonZeroUsize,
    pub is_order_completed: bool,
}

#[derive(Debug, Clone)]
pub struct OrderMatcher<K> {
    price_queues: PriceQueues<K>,
    reused_queues: Vec<PriceQueue<K>>,
}
impl<K> OrderMatcher<K> {
    pub fn new() -> Self {
        Self {
            price_queues: PriceQueues::new(),
            reused_queues: vec![],
        }
    }
}
impl<K> Clear for OrderMatcher<K> {
    fn clear(&mut self) {
        while let Some((_k, v)) = self.price_queues.ask_queues.pop_first() {
            self.reused_queues.push(v);
        }
        while let Some((_k, v)) = self.price_queues.bid_queues.pop_first() {
            self.reused_queues.push(v);
        }
    }
}
impl<K: Clone> OrderMatcher<K> {
    pub fn cancel_order(&mut self, side: Side, price: UnitPrice, index: QueueIndex) {
        let queue = self.price_queues.get_mut(side, price).unwrap();
        queue.cancel(index);
    }

    pub fn place_limit_order(
        &mut self,
        order: LimitOrder<K>,
        on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<QueueIndex> {
        let remaining_quantity =
            self.fill_orders(order.side, order.price, order.quantity, on_each_filled)?;
        let order = LimitOrder {
            quantity: remaining_quantity,
            ..order
        };
        Some(self.insert_order(order))
    }

    /// Return remaining quantity
    pub fn place_market_order(
        &mut self,
        side: Side,
        quantity: NonZeroUsize,
        on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<NonZeroUsize> {
        let price = match side {
            Side::Buy => NonZeroUsize::new(usize::MAX).unwrap(),
            Side::Sell => NonZeroUsize::new(1).unwrap(),
        };
        let price = UnitPrice(price);
        self.fill_orders(side, price, quantity, on_each_filled)
    }

    /// Return remaining quantity
    fn fill_orders(
        &mut self,
        side: Side,
        limit_price: UnitPrice,
        quantity: NonZeroUsize,
        mut on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<NonZeroUsize> {
        let mut remaining_quantity = Some(quantity);
        while let Some(quantity) = remaining_quantity {
            let best_matchable = self
                .price_queues
                .best_matchable_queue_mut(side, limit_price);
            let Some((price, queue)) = best_matchable else {
                break;
            };
            remaining_quantity = queue.fill_all(quantity, &mut on_each_filled);
            if queue.is_empty() {
                self.reused_queues
                    .push(self.price_queues.remove(side.flip(), price).unwrap());
            }
        }
        remaining_quantity
    }

    fn insert_order(&mut self, order: LimitOrder<K>) -> QueueIndex {
        let queue = self
            .price_queues
            .ensure_get_mut(order.side, order.price, || {
                if let Some(queue) = self.reused_queues.pop() {
                    return queue;
                }
                PriceQueue::new()
            });
        queue.push(order.name, order.quantity)
    }

    #[allow(dead_code)]
    pub(crate) fn check_rep(&self) {
        check_spread(&self.price_queues);
        check_empty_queue(&self.price_queues);
        fn check_spread<K>(price_queues: &PriceQueues<K>) {
            let Some(best_ask) = price_queues.ask_queues.iter().next() else {
                return;
            };
            let Some(best_bid) = price_queues.bid_queues.iter().next() else {
                return;
            };
            assert!(best_bid.0 .0 < *best_ask.0);
        }
        fn check_empty_queue<K: Clone>(price_queues: &PriceQueues<K>) {
            for (_, queue) in price_queues.ask_queues.iter() {
                check_queue(queue);
            }
            for (_, queue) in price_queues.bid_queues.iter() {
                check_queue(queue);
            }
            fn check_queue<K: Clone>(queue: &PriceQueue<K>) {
                assert!(!queue.is_empty());
            }
        }
    }
}
impl<K> Default for OrderMatcher<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct PriceQueues<K> {
    ask_queues: LinearFrontBTreeMap11<UnitPrice, PriceQueue<K>>,
    bid_queues: LinearFrontBTreeMap11<Reverse<UnitPrice>, PriceQueue<K>>,
}
impl<K> PriceQueues<K> {
    pub fn new() -> Self {
        Self {
            ask_queues: LinearFrontBTreeMap11::new(),
            bid_queues: LinearFrontBTreeMap11::new(),
        }
    }
    pub fn get_mut(&mut self, side: Side, price: UnitPrice) -> Option<&mut PriceQueue<K>> {
        match side {
            Side::Buy => self.bid_queues.get_mut(&Reverse(price)),
            Side::Sell => self.ask_queues.get_mut(&price),
        }
    }
    pub fn ensure_get_mut(
        &mut self,
        side: Side,
        price: UnitPrice,
        mut new: impl FnMut() -> PriceQueue<K>,
    ) -> &mut PriceQueue<K> {
        match side {
            Side::Buy => {
                if self.bid_queues.get(&Reverse(price)).is_none() {
                    self.bid_queues.insert(Reverse(price), new());
                }
                self.bid_queues.get_mut(&Reverse(price)).unwrap()
            }
            Side::Sell => {
                if self.ask_queues.get(&price).is_none() {
                    self.ask_queues.insert(price, new());
                }
                self.ask_queues.get_mut(&price).unwrap()
            }
        }
    }
    pub fn best_matchable_queue_mut(
        &mut self,
        side: Side,
        limit_price: UnitPrice,
    ) -> Option<(UnitPrice, &mut PriceQueue<K>)> {
        match side {
            Side::Buy => self
                .ask_queues
                .iter_mut()
                .nth(0)
                .filter(|(ask_price, _)| **ask_price <= limit_price)
                .map(|(price, queue)| (*price, queue)),
            Side::Sell => self
                .bid_queues
                .iter_mut()
                .nth(0)
                .filter(|(Reverse(bid_price), _)| limit_price <= *bid_price)
                .map(|(Reverse(price), queue)| (*price, queue)),
        }
    }
    pub fn remove(&mut self, side: Side, price: UnitPrice) -> Option<PriceQueue<K>> {
        match side {
            Side::Buy => self.bid_queues.remove(&Reverse(price)),
            Side::Sell => self.ask_queues.remove(&price),
        }
    }
}

#[derive(Debug, Clone)]
struct PriceQueue<K> {
    orders: IndQueue<UnfilledOrder<K>>,
}
impl<K> PriceQueue<K> {
    pub fn new() -> Self {
        Self {
            orders: IndQueue::new(),
        }
    }
}
impl<K: Clone> PriceQueue<K> {
    pub fn push(&mut self, name: K, quantity: NonZeroUsize) -> QueueIndex {
        self.orders.enqueue(UnfilledOrder::new(name, quantity))
    }
    pub fn cancel(&mut self, index: QueueIndex) {
        self.orders.remove(index);
    }
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
    pub fn fill_one(&mut self, quantity: NonZeroUsize) -> Option<(Filled<K>, usize)> {
        let front = self.orders.front_mut()?;
        let (remaining, neural, front_quantity) =
            neutralize_quantity(quantity.get(), front.quantity.get());
        let name = front.name.clone();
        let front_quantity = NonZeroUsize::new(front_quantity);
        if let Some(front_quantity) = front_quantity {
            front.quantity = front_quantity;
        };
        let is_order_completed = front_quantity.is_none();
        if is_order_completed {
            self.orders.dequeue().unwrap();
        }
        let filled = Filled {
            name,
            quantity: NonZeroUsize::new(neural).unwrap(),
            is_order_completed,
        };
        Some((filled, remaining))
    }
    /// Return the remaining quantity
    pub fn fill_all(
        &mut self,
        quantity: NonZeroUsize,
        mut on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<NonZeroUsize> {
        let mut remaining_quantity = quantity;
        while let Some((filled, remaining)) = self.fill_one(remaining_quantity) {
            on_each_filled(filled);
            let remaining = NonZeroUsize::new(remaining)?;
            remaining_quantity = remaining;
        }
        assert!(self.is_empty());
        Some(remaining_quantity)
    }
}

#[derive(Debug, Clone)]
struct UnfilledOrder<K> {
    pub name: K,
    pub quantity: NonZeroUsize,
}
impl<K> UnfilledOrder<K> {
    pub fn new(name: K, quantity: NonZeroUsize) -> Self {
        Self { name, quantity }
    }
}

fn neutralize_quantity(a: usize, b: usize) -> (usize, usize, usize) {
    let min = a.min(b);
    (a - min, min, b - min)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_place_cancel() {
        let instructions = [
            (0, Side::Buy, 2, 2, &[][..], &[][..]),
            (1, Side::Buy, 2, 2, &[], &[]),
            (2, Side::Buy, 2, 2, &[], &[2]),
            (3, Side::Buy, 1, 2, &[], &[]),
            (4, Side::Sell, 1, 3, &[(0, 2, true), (1, 1, false)], &[]),
            (5, Side::Sell, 1, 3, &[(1, 1, true), (3, 2, true)], &[]),
        ];
        let mut matcher = OrderMatcher::new();
        let mut filled_buf = vec![];
        fn push<T>(buf: &mut Vec<T>) -> impl FnMut(T) + '_ {
            |item| {
                buf.push(item);
            }
        }
        let mut orders = HashMap::new();
        for instruction in instructions {
            let name = instruction.0;
            let side = instruction.1;
            let price = UnitPrice::new(NonZeroUsize::new(instruction.2).unwrap());
            let order = LimitOrder {
                name,
                side,
                price,
                quantity: NonZeroUsize::new(instruction.3).unwrap(),
            };
            let index = matcher.place_limit_order(order, push(&mut filled_buf));
            if let Some(index) = index {
                orders.insert(name, (side, price, index));
            }
            assert_eq!(filled_buf.len(), instruction.4.len());
            for (a, b) in filled_buf.iter().zip(instruction.4) {
                assert_eq!(a.name, b.0);
                assert_eq!(a.quantity.get(), b.1);
                assert_eq!(a.is_order_completed, b.2);
            }
            for name in instruction.5 {
                let (side, price, index) = orders.remove(name).unwrap();
                matcher.cancel_order(side, price, index);
            }
            filled_buf.clear();
            matcher.check_rep();
        }
    }
}
