use std::{
    collections::HashMap,
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    mem::MaybeUninit,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use parking_lot::Mutex;

use super::Cache;

#[derive(Clone, Copy)]
struct KeyRef<K> {
    k: *const K,
}

impl<K> From<&K> for KeyRef<K> {
    fn from(value: &K) -> Self {
        Self { k: value }
    }
}

impl<K> Default for KeyRef<K> {
    fn default() -> Self {
        Self { k: ptr::null() }
    }
}

impl<K> Debug for KeyRef<K>
where
    K: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { f.debug_struct("KeyRef").field("k", &*self.k).finish() }
    }
}

impl<K> Hash for KeyRef<K>
where
    K: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.k).hash(state);
        }
    }
}

impl<K> PartialEq for KeyRef<K>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K> Eq for KeyRef<K> where K: Eq {}

struct LruEntry<K, V> {
    charge: usize,
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    prev: *mut Self,
    next: *mut Self,
}

impl<K, V> Default for LruEntry<K, V> {
    fn default() -> Self {
        Self {
            charge: 0,
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

impl<K, V> LruEntry<K, V> {
    fn new(key: K, value: V, charge: usize) -> Self {
        Self {
            charge,
            key: MaybeUninit::new(key),
            value: MaybeUninit::new(value),
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

struct LruCacheInner<K, V> {
    table: HashMap<KeyRef<K>, Box<LruEntry<K, V>>>,
    head: *mut LruEntry<K, V>,
    tail: *mut LruEntry<K, V>,
}

impl<K, V> LruCacheInner<K, V> {
    fn remove_node(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            let prev = (*node).prev;
            let next = (*node).next;
            (*prev).next = next;
            (*next).prev = prev;
            // (*node).prev = std::ptr::null_mut();
            // (*node).next = std::ptr::null_mut();
        }
    }

    fn push_node(&mut self, node: *mut LruEntry<K, V>) {
        unsafe {
            // head and tail is not null.
            let head = self.head;
            let next = (*head).next;
            (*node).prev = head;
            (*node).next = next;
            (*head).next = node;
            (*next).prev = node;
        }
    }

    fn update_node(&mut self, node: *mut LruEntry<K, V>) {
        self.remove_node(node);
        self.push_node(node);
    }
}

pub struct LruCache<K, V> {
    inner: Arc<Mutex<LruCacheInner<K, V>>>,
    usage: Arc<AtomicUsize>,
    cap: usize,
}

unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LruCache<K, V> {}

impl<K, V> LruCache<K, V>
where
    K: Hash + Eq,
{
    pub fn new(cap: usize) -> Self {
        unsafe {
            let head = Box::into_raw(Box::new(LruEntry::default()));
            let tail = Box::into_raw(Box::new(LruEntry::default()));
            (*head).next = tail;
            (*tail).prev = head;

            let inner = LruCacheInner {
                table: HashMap::new(),
                head,
                tail,
            };

            Self {
                inner: Arc::new(Mutex::new(inner)),
                usage: Arc::new(AtomicUsize::new(0)),
                cap,
            }
        }
    }
}

impl<K, V> Cache<K, V> for LruCache<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync,
{
    fn insert(&self, key: K, mut value: V, charge: usize) -> Option<V> {
        tracing::debug!(
            "insert charge: {}, remainning charge: {}",
            charge,
            self.cap as isize - self.total_charge() as isize
        );

        if self.cap == 0 {
            return None;
        }

        let mut inner = self.inner.lock();
        let old_value = match inner.table.get_mut(&KeyRef::from(&key)) {
            Some(node) => {
                unsafe {
                    core::ptr::swap(&mut value, node.as_mut().value.as_mut_ptr());
                }
                self.usage.fetch_sub(node.charge, Ordering::Relaxed);
                self.usage.fetch_add(charge, Ordering::Relaxed);
                node.as_mut().charge = charge;

                let ptr = node.as_mut() as *mut _;
                inner.update_node(ptr);
                Some(value)
            }
            None => {
                let mut node = Box::new(LruEntry::new(key, value, charge));
                let ptr = node.as_mut() as *mut _;
                inner.push_node(ptr);
                let key_ref = KeyRef::from(unsafe { &*node.key.as_ptr() });
                inner.table.insert(key_ref, node);
                self.usage.fetch_add(charge, Ordering::Relaxed);
                None
            }
        };

        unsafe {
            while self.total_charge() > self.cap && !ptr::eq((*inner.head).next, inner.tail) {
                let node = (*inner.tail).prev;
                inner.remove_node(node);
                let k = KeyRef::from(&(*(*node).key.as_ptr()));
                let node_box = inner.table.remove(&k);
                self.usage.fetch_sub((*node).charge, Ordering::Relaxed);
                (*node).key.assume_init_drop();
                (*node).value.assume_init_drop();
                drop(node_box);
            }
        }

        old_value
    }

    fn get(&self, key: &K) -> Option<&V> {
        let key_ref = KeyRef::from(key);
        let mut inner = self.inner.lock();
        if let Some(node) = inner.table.get_mut(&key_ref) {
            let node_ptr = node.as_mut() as *mut _;
            inner.update_node(node_ptr);
            Some(unsafe { &*((*node_ptr).value.as_ptr()) })
        } else {
            None
        }
    }

    fn erase(&self, key: &K) -> Option<V> {
        let k = KeyRef::from(key);
        let mut inner = self.inner.lock();
        if let Some(mut node) = inner.table.remove(&k) {
            self.usage.fetch_sub(node.charge, Ordering::Relaxed);
            inner.remove_node(node.as_mut());
            unsafe {
                node.key.assume_init_drop();
                Some(node.value.assume_init())
            }
        } else {
            None
        }
    }

    fn total_charge(&self) -> usize {
        self.usage.load(Ordering::Relaxed)
    }
}

impl<K, V> Drop for LruCacheInner<K, V> {
    fn drop(&mut self) {
        unsafe {
            for node in self.table.values_mut() {
                node.key.assume_init_drop();
                node.value.assume_init_drop();
            }
            drop(Box::from_raw(self.head));
            drop(Box::from_raw(self.tail));
        }
    }
}

pub struct SharededLruCache<K, V> {
    caches: Arc<Vec<LruCache<K, V>>>,
}

impl<K, V> SharededLruCache<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync,
    // H: Hasher + Default,
{
    fn shared_hash(&self, hash: usize) -> usize {
        hash % self.caches.len()
    }

    fn get_cache(&self, key: &K) -> &LruCache<K, V> {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let index = self.shared_hash(hasher.finish() as usize);
        &self.caches[index]
    }

    pub fn new(len: usize, per_cap: usize) -> Self {
        let mut caches = Vec::with_capacity(len);
        for _ in 0..len {
            caches.push(LruCache::new(per_cap));
        }
        Self {
            caches: Arc::new(caches),
        }
    }
}

impl<K, V> Cache<K, V> for SharededLruCache<K, V>
where
    K: Send + Sync + Hash + Eq,
    V: Send + Sync,
    // H: Hasher + Default + Send + Sync,
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        self.get_cache(&key).insert(key, value, charge)
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.get_cache(key).get(key)
    }

    fn erase(&self, key: &K) -> Option<V> {
        self.get_cache(key).erase(key)
    }

    fn total_charge(&self) -> usize {
        self.caches.iter().map(|c| c.total_charge()).sum()
    }
}

#[cfg(test)]
mod tests {
    use crate::caches::{Cache, lru_cache::LruCache};

    #[test]
    fn test_empty_cache() {
        let cache: LruCache<i32, i32> = LruCache::new(0);
        assert_eq!(cache.total_charge(), 0);
    }

    #[test]
    fn test_cache_charge() {
        const TEST_COUNT: usize = 100000;

        let cache = LruCache::new(TEST_COUNT);
        for i in 0..TEST_COUNT {
            cache.insert(i, i, 1);
        }
        assert_eq!(cache.total_charge(), TEST_COUNT);

        for i in 0..TEST_COUNT {
            let res = cache.erase(&i);
            assert_eq!(res, Some(i));
        }
    }

    #[test]
    fn test_cache_lru() {
        const TEST_COUNT: usize = 200;

        let cache = LruCache::new(TEST_COUNT / 2);
        for i in 0..TEST_COUNT / 2 {
            let res = cache.insert(i, i, 1);
            assert_eq!(res, None);
            assert_eq!(cache.total_charge(), i + 1);
        }
        for i in TEST_COUNT / 2..TEST_COUNT {
            let res = cache.insert(i, i, 1);
            assert_eq!(res, None);
            assert_eq!(cache.total_charge(), TEST_COUNT / 2);
        }

        for i in 0..TEST_COUNT / 2 {
            let res = cache.get(&i);
            assert_eq!(res, None);
        }
        for i in TEST_COUNT / 2..TEST_COUNT {
            let res = cache.get(&i);
            assert_eq!(res, Some(&i));
        }
    }
}
