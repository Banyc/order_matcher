use core::{cmp::Reverse, num::NonZeroUsize};

use primitive::{
    map::{linear_front_btree::LinearFrontBTreeMap11, MapInsert},
    ops::{clear::Clear, len::LenExt},
    queue::ind_queue::{IndQueue, QueueIndex},
};

#[derive(Debug, Clone)]
pub struct Order<K> {
    pub name: K,
    pub side: Side,
    pub limit_price: UnitPrice,
    pub size: NonZeroUsize,
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
        if queue.is_empty() {
            self.reused_queues
                .push(self.price_queues.remove(side, price).unwrap());
        }
    }

    pub fn place_order(
        &mut self,
        order: Order<K>,
        on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<QueueIndex> {
        let new_size =
            self.fill_orders(order.side, order.limit_price, order.size, on_each_filled)?;
        let order = Order {
            size: new_size,
            ..order
        };
        Some(self.insert_order(order))
    }

    /// Return remaining size
    pub fn place_market_order(
        &mut self,
        side: Side,
        size: NonZeroUsize,
        on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<NonZeroUsize> {
        let price = UnitPrice::new(match side {
            Side::Buy => NonZeroUsize::new(usize::MAX).unwrap(),
            Side::Sell => NonZeroUsize::new(1).unwrap(),
        });
        self.fill_orders(side, price, size, on_each_filled)
    }

    /// Return remaining size
    fn fill_orders(
        &mut self,
        side: Side,
        limit_price: UnitPrice,
        quantity: NonZeroUsize,
        mut on_each_filled: impl FnMut(Filled<K>),
    ) -> Option<NonZeroUsize> {
        let mut remaining_quantity = Some(quantity);
        while let Some(quantity) = remaining_quantity {
            let Some((price, queue)) = self
                .price_queues
                .eligible_highest_priority_queue_mut(side, limit_price)
            else {
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

    fn insert_order(&mut self, order: Order<K>) -> QueueIndex {
        let queue = self
            .price_queues
            .ensure_get_mut(order.side, order.limit_price, || {
                if let Some(queue) = self.reused_queues.pop() {
                    return queue;
                }
                PriceQueue::new()
            });
        queue.push(order.name, order.size)
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
    pub fn eligible_highest_priority_queue_mut(
        &mut self,
        incoming_side: Side,
        limit_price: UnitPrice,
    ) -> Option<(UnitPrice, &mut PriceQueue<K>)> {
        match incoming_side {
            Side::Buy => self
                .ask_queues
                .iter_mut()
                .nth(0)
                .filter(|(&ask_price, _)| ask_price <= limit_price)
                .map(|(&price, queue)| (price, queue)),
            Side::Sell => self
                .bid_queues
                .iter_mut()
                .nth(0)
                .filter(|(&Reverse(bid_price), _)| limit_price <= bid_price)
                .map(|(&Reverse(price), queue)| (price, queue)),
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
    orders: IndQueue<ActiveOrder<K>>,
}
impl<K> PriceQueue<K> {
    pub fn new() -> Self {
        Self {
            orders: IndQueue::new(),
        }
    }
}
impl<K: Clone> PriceQueue<K> {
    pub fn push(&mut self, name: K, size: NonZeroUsize) -> QueueIndex {
        self.orders.enqueue(ActiveOrder::new(name, size))
    }
    pub fn cancel(&mut self, index: QueueIndex) {
        self.orders.remove(index);
    }
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
    pub fn fill_one(&mut self, size: NonZeroUsize) -> Option<(Filled<K>, usize)> {
        let front = self.orders.front_mut()?;
        let (remaining, matched_quantity, new_front_quantity) =
            match_sizes(size.get(), front.quantity.get());
        let name = front.name.clone();
        let new_front_quantity = NonZeroUsize::new(new_front_quantity);
        if let Some(quantity) = new_front_quantity {
            front.quantity = quantity;
        };
        let is_order_completed = new_front_quantity.is_none();
        if is_order_completed {
            self.orders.dequeue().unwrap();
        }
        let filled = Filled {
            name,
            quantity: NonZeroUsize::new(matched_quantity).unwrap(),
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
        while let Some((filled, new_quantity)) = self.fill_one(remaining_quantity) {
            on_each_filled(filled);
            remaining_quantity = NonZeroUsize::new(new_quantity)?;
        }
        assert!(self.is_empty());
        Some(remaining_quantity)
    }
}

#[derive(Debug, Clone)]
struct ActiveOrder<K> {
    pub name: K,
    pub quantity: NonZeroUsize,
}
impl<K> ActiveOrder<K> {
    pub fn new(name: K, size: NonZeroUsize) -> Self {
        Self {
            name,
            quantity: size,
        }
    }
}

fn match_sizes(a: usize, b: usize) -> (usize, usize, usize) {
    let min = a.min(b);
    (a - min, min, b - min)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::{distributions::Standard, prelude::Distribution, thread_rng, Rng};

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
        let mut orders = HashMap::new();
        for instruction in instructions {
            let name = instruction.0;
            let side = instruction.1;
            let price = UnitPrice::new(NonZeroUsize::new(instruction.2).unwrap());
            let order = Order {
                name,
                side,
                limit_price: price,
                size: NonZeroUsize::new(instruction.3).unwrap(),
            };
            let index = matcher.place_order(order, push(&mut filled_buf));
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

    #[test]
    fn test_simulated() {
        let rounds = if cfg!(debug_assertions) {
            1 << 10
        } else {
            1 << 16
        };
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(move || {
                    let mut rng = thread_rng();
                    for _ in 0..(1 << 8) {
                        simulate(&mut rng, rounds);
                    }
                });
            }
        });
    }
    fn simulate(rng: &mut impl Rng, rounds: usize) {
        let mut matcher = OrderMatcher::new();
        let mut filled_buf = vec![];
        let mut orders = HashMap::new();
        let price_range = || 1..20;
        let quantity_range = || 1..100;
        for i in 0..rounds {
            filled_buf.clear();
            let action: SimulateAction = rng.gen();
            match action {
                SimulateAction::Limit => {
                    let price = rng.gen_range(price_range());
                    let price = UnitPrice::new(NonZeroUsize::new(price).unwrap());
                    let side: Side = rng.gen();
                    let is_filled = match side {
                        Side::Buy => {
                            let best_ask = matcher
                                .price_queues
                                .ask_queues
                                .iter()
                                .next()
                                .map(|(p, _)| *p);
                            best_ask.map(|p| p <= price).unwrap_or(false)
                        }
                        Side::Sell => {
                            let best_bid = matcher
                                .price_queues
                                .bid_queues
                                .iter()
                                .next()
                                .map(|(p, _)| p.0);
                            best_bid.map(|p| price <= p).unwrap_or(false)
                        }
                    };
                    let size = rng.gen_range(quantity_range());
                    let size = NonZeroUsize::new(size).unwrap();
                    let order = Order {
                        name: i,
                        side,
                        limit_price: price,
                        size,
                    };
                    let index = matcher.place_order(order, push(&mut filled_buf));
                    if let Some(index) = index {
                        orders.insert(i, (side, price, index));
                    }
                    assert_eq!(!filled_buf.is_empty(), is_filled);
                }
                SimulateAction::Market => {
                    let side: Side = rng.gen();
                    let size = rng.gen_range(quantity_range());
                    let size = NonZeroUsize::new(size).unwrap();
                    let remaining = matcher.place_market_order(side, size, push(&mut filled_buf));
                    if remaining == Some(size) {
                        assert!(filled_buf.is_empty());
                    } else {
                        assert!(!filled_buf.is_empty());
                    }
                }
                SimulateAction::Cancel => {
                    let Some(&name) = orders.keys().next() else {
                        continue;
                    };
                    let (side, price, index) = orders.remove(&name).unwrap();
                    matcher.cancel_order(side, price, index);
                }
            }
            for filled in &filled_buf {
                if filled.is_order_completed {
                    orders.remove(&filled.name).unwrap();
                }
            }
            matcher.check_rep();
        }
    }

    impl Distribution<Side> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Side {
            match rng.gen_range(0..2) {
                0 => Side::Buy,
                _ => Side::Sell,
            }
        }
    }

    enum SimulateAction {
        Limit,
        Market,
        Cancel,
    }
    impl Distribution<SimulateAction> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> SimulateAction {
            match rng.gen_range(0..3) {
                0 => SimulateAction::Limit,
                1 => SimulateAction::Market,
                _ => SimulateAction::Cancel,
            }
        }
    }

    fn push<T>(buf: &mut Vec<T>) -> impl FnMut(T) + '_ {
        |item| {
            buf.push(item);
        }
    }
}
