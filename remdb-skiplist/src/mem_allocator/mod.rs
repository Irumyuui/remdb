use std::{
    alloc::Layout,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

pub mod block_arena;
pub mod vec_arena;

pub trait MemAllocator {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8;
    fn mem_usage(&self) -> usize;
}

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

impl MemAllocator for DefaultAllocatorInner {
    unsafe fn allocate(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::alloc(layout) };
        self.mems.lock().unwrap().push((ptr, layout));
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
            for (ptr, layout) in self.mems.get_mut().unwrap().iter() {
                std::alloc::dealloc(*ptr, *layout);
            }
        }
    }
}
