use std::{
    alloc::Layout,
    sync::{Arc, atomic::AtomicUsize},
};

use parking_lot::Mutex;

use super::MemAllocator;

#[derive(Default, Debug, Clone)]
pub struct DefaultAllocator(Arc<DefaultAllocatorInner>);

impl MemAllocator for DefaultAllocator {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        unsafe { self.0.allocate(layout) }
    }

    fn mem_usage(&self) -> usize {
        self.0.mem_usage()
    }
}

#[derive(Default, Debug)]
pub struct DefaultAllocatorInner {
    mems: Mutex<Vec<(*mut u8, Layout)>>,
    mem_alloc: AtomicUsize,
}

unsafe impl Sync for DefaultAllocatorInner {}
unsafe impl Send for DefaultAllocatorInner {}

impl MemAllocator for DefaultAllocatorInner {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::alloc(layout) };
        self.mems.lock().push((ptr, layout));
        self.mem_alloc
            .fetch_add(layout.size(), std::sync::atomic::Ordering::SeqCst);
        ptr
    }

    fn mem_usage(&self) -> usize {
        self.mem_alloc.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for DefaultAllocatorInner {
    fn drop(&mut self) {
        unsafe {
            for (ptr, layout) in self.mems.get_mut().iter() {
                std::alloc::dealloc(*ptr, *layout);
            }
        }
    }
}
