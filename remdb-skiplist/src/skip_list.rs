use std::{
    alloc::Layout,
    fmt::Debug,
    ops::Index,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicUsize, Ordering::*},
    },
};

use crate::mem_allocator::MemAllocator;

pub mod prelude {
    pub use super::SkipList;
    pub use super::SkipListIter;
}

const MAX_HEIGHT: usize = 12;

#[repr(C)]
struct Node<K: Ord> {
    key: K,
    next: [AtomicPtr<Self>; 0], // fuck rust
}

impl<K> Index<usize> for Node<K>
where
    K: Ord,
{
    type Output = AtomicPtr<Self>;

    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.next.as_ptr().add(index) }
    }
}

impl<K> Node<K>
where
    K: Ord,
{
    fn next(&self, index: usize) -> *mut Self {
        self[index].load(Acquire)
    }

    fn set_next(&self, index: usize, new_next: *mut Self) {
        self[index].store(new_next, Release);
    }

    fn get_layout(height: usize) -> Layout {
        Layout::new::<Self>()
            .extend(Layout::array::<AtomicPtr<Self>>(height).unwrap())
            .unwrap()
            .0
            .pad_to_align()
    }

    unsafe fn new_node(allocator: &impl MemAllocator, key: K, height: usize) -> *mut Self {
        unsafe {
            let layout = Self::get_layout(height);
            let ptr = allocator.allocate(layout) as *mut Self;

            let node = &mut *ptr;
            std::ptr::write(&mut node.key, key);
            std::ptr::write_bytes(&mut node.next, 0, height);

            ptr
        }
    }

    // skip key init...
    unsafe fn new_head(allocator: &impl MemAllocator, height: usize) -> *mut Self {
        let layout = Self::get_layout(height);
        unsafe {
            let ptr = allocator.allocate(layout) as *mut Self;

            let node = &mut *ptr;
            std::ptr::write_bytes(&mut node.next, 0, height);
            ptr
        }
    }
}

pub struct SkipList<K: Ord, A: MemAllocator> {
    head: AtomicPtr<Node<K>>,
    allocator: A,
    max_height: AtomicUsize,
}

impl<K, A> Default for SkipList<K, A>
where
    K: Ord,
    A: MemAllocator + Default,
{
    fn default() -> Self {
        Self::new(A::default())
    }
}

impl<K, A> SkipList<K, A>
where
    K: Ord,
    A: MemAllocator,
{
    pub fn new(allocator: A) -> Self {
        unsafe {
            let head = Node::<K>::new_head(&allocator, MAX_HEIGHT);

            let this = Self {
                head: AtomicPtr::new(head),
                allocator,
                max_height: AtomicUsize::new(1),
            };
            this
        }
    }

    pub fn insert(&self, key: K) {
        let mut prev = [std::ptr::null_mut(); MAX_HEIGHT];
        let node = self.find_greater_or_equal(&key, Some(&mut prev));

        unsafe {
            assert!(node.is_null() || (*node).key != key);

            // update new height
            let height = random_height();
            let cur_max_height = self.max_height();
            if height > cur_max_height {
                for i in cur_max_height..height {
                    prev[i] = self.head.load(Relaxed);
                }
                self.max_height.store(height, Release);
            }

            let new_node = Node::new_node(
                &self.allocator, // require thread safety
                key,
                height,
            );

            for i in 0..height {
                (*new_node).set_next(i, (*prev[i]).next(i));
                (*prev[i]).set_next(i, new_node);
            }
        }
    }

    pub fn contains(&self, key: &K) -> bool {
        let node = self.find_greater_or_equal(key, None);
        unsafe { !node.is_null() && &(*node).key == key }
    }

    fn max_height(&self) -> usize {
        self.max_height.load(Acquire)
    }

    fn find_greater_or_equal(
        &self,
        key: &K,
        mut prev: Option<&mut [*mut Node<K>]>,
    ) -> *mut Node<K> {
        unsafe {
            let mut cur = self.head.load(Relaxed);
            let mut level = self.max_height() - 1;

            loop {
                let next = (*cur).next(level);
                if !next.is_null() && &(*next).key < key {
                    cur = next;
                } else {
                    if let Some(prev) = prev.as_mut() {
                        prev[level] = cur;
                    }
                    if level == 0 {
                        return next;
                    }
                    level -= 1;
                }
            }
        }
    }

    fn find_less_than(&self, key: &K) -> *mut Node<K> {
        unsafe {
            let mut cur = self.head.load(Relaxed);
            let mut level = self.max_height() - 1;

            loop {
                let next = (*cur).next(level);
                if next.is_null() || key >= &(*next).key {
                    if level == 0 {
                        return cur;
                    }
                    level -= 1;
                } else {
                    cur = next;
                }
            }
        }
    }

    fn find_last(&self) -> *mut Node<K> {
        unsafe {
            let mut cur = self.head.load(Relaxed);
            let mut level = self.max_height() - 1;
            loop {
                let next = (*cur).next(level);
                if !next.is_null() {
                    cur = next;
                } else {
                    if level == 0 {
                        return cur;
                    }
                    level -= 1;
                }
            }
        }
    }

    pub fn mem_usage(&self) -> usize {
        self.allocator.mem_usage()
    }
}

impl<K, A> Drop for SkipList<K, A>
where
    K: Ord,
    A: MemAllocator,
{
    fn drop(&mut self) {
        unsafe {
            let mut cur = self.head.load(Relaxed);
            while !cur.is_null() {
                let next = (*cur).next(0);
                std::ptr::drop_in_place(cur);
                cur = next;
            }
        }
    }
}

// [1, MAX_HEIGHT]
fn random_height() -> usize {
    const UPGRADE: usize = 4;
    let mut h = 1;
    while h < MAX_HEIGHT && (rand::random::<u32>() as usize % UPGRADE) == 0 {
        h += 1;
    }
    h
}

impl<T, A> Debug for SkipList<T, A>
where
    T: Ord + Debug,
    A: MemAllocator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut vec = vec![];
        unsafe {
            let mut cur = (*self.head.load(Relaxed)).next(0);
            while !cur.is_null() {
                vec.push(&(*cur).key);
                cur = (*cur).next(0);
            }
        }

        f.debug_struct("SkipList")
            .field("mem_usage", &self.mem_usage())
            .field("key", &vec)
            .finish()
    }
}

pub struct SkipListIter<K: Ord, A: MemAllocator> {
    skl: Arc<SkipList<K, A>>,
    node: *const Node<K>,
}

impl<K, A> SkipListIter<K, A>
where
    K: Ord,
    A: MemAllocator,
{
    pub fn new(skl: Arc<SkipList<K, A>>) -> Self {
        Self {
            skl,
            node: std::ptr::null(),
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.node.is_null()
    }

    pub fn peek(&self) -> Option<&K> {
        if self.is_valid() {
            unsafe { Some(&(*self.node).key) }
        } else {
            None
        }
    }

    pub fn prev(&mut self) {
        assert!(self.is_valid());
        self.node = self.skl.find_less_than(self.peek().unwrap());
    }

    pub fn next(&mut self) {
        assert!(self.is_valid());
        self.node = unsafe { (*self.node).next(0) };
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            self.node = (*self.skl.head.load(Relaxed)).next(0);
        }
    }

    pub fn seek_to_last(&mut self) {
        let last = self.skl.find_last();
        self.node = last;
    }

    pub fn seek(&mut self, target: &K) {
        let target = self.skl.find_greater_or_equal(target, None);
        self.node = target;
    }

    // pub fn status(&mut self) -> <()> {
    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{mem_allocator::DefaultAllocator, skip_list::SkipListIter};

    use super::SkipList;

    #[test]
    fn insert_some() {
        let list = SkipList::new(DefaultAllocator::default());
        for i in 0..1000 {
            list.insert(i);
        }
        for i in 0..1000 {
            assert!(list.contains(&i));
        }
    }

    #[test]
    fn iterator() {
        let list = SkipList::new(DefaultAllocator::default());
        let list = Arc::new(list);

        for i in 0..1000 {
            list.insert(i);
        }

        let mut iter = SkipListIter::new(list);
        iter.seek_to_last();
        iter.seek_to_first();
        for i in 0..1000 {
            assert_eq!(iter.peek().unwrap(), &i);
            iter.next();
        }
        assert!(!iter.is_valid());

        for i in 0..1000 {
            iter.seek(&i);
            assert_eq!(iter.peek().unwrap(), &i);
        }
    }
}
