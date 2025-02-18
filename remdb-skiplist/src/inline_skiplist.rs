// use std::{ptr::NonNull, sync::atomic::AtomicUsize};

// use crate::arena::VecArena;

// pub trait KeyComaprator: Send + Sync + Clone {
//     type Key;

//     fn compare(&self, a: &Self::Key, b: &Self::Key) -> std::cmp::Ordering;
// }

// const MAX_HEIGHT: usize = 20;

// struct Node<K, V> {
//     key: K,
//     value: V,
//     height: usize,
//     tower: [AtomicUsize; MAX_HEIGHT],
// }

// struct SkipListInner<K, V> {
//     height: AtomicUsize,
//     head: NonNull<Node<K, V>>,
//     arena: VecArena,
// }

// pub struct SkipList<K, V, C>
// where
//     C: KeyComaprator<Key = K>,
// {
//     c: C,
// }
