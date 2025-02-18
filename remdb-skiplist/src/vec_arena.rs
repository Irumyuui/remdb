#![allow(unused)]

use std::{
    cell::Cell,
    sync::atomic::{AtomicUsize, Ordering},
};

use crossbeam::epoch::Guard;

pub struct VecArena {
    ptr: Cell<*mut u8>,
    cap: Cell<usize>,
    len: AtomicUsize,
}

type Allcator = Vec<u64>;

impl VecArena {
    pub fn with_capacity(cap: usize) -> VecArena {
        let mut mem = Allcator::with_capacity(cap / 8);

        let ptr = mem.as_mut_ptr() as *mut u8;
        let cap = mem.capacity() * 8;
        std::mem::forget(mem);

        VecArena {
            ptr: Cell::new(ptr),
            cap: Cell::new(cap),
            len: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    pub fn alloc(&self, size: usize, guard: &Guard) -> usize {
        let size = (size + 7) / 8;
        let offset = self.len.fetch_add(size, Ordering::SeqCst);
        let old_cap = self.cap.get();

        if offset + size > old_cap {
            let need_size = (offset + size).max(old_cap * 2);

            let mut new_mem = Allcator::with_capacity((need_size + 7) / 8);
            let new_ptr = new_mem.as_mut_ptr() as *mut u8;
            let new_cap = new_mem.capacity() * 8;
            std::mem::forget(new_mem);

            let old_ptr = self.ptr.get();
            unsafe {
                std::ptr::copy_nonoverlapping(old_ptr, new_ptr, old_cap);
            }

            let fake_ptr = old_ptr as usize;
            guard.defer(move || unsafe {
                let _ = Allcator::from_raw_parts(fake_ptr as _, 0, old_cap / 8);
            });

            self.ptr.set(new_ptr);
            self.cap.set(new_cap);
        }

        offset
    }

    pub unsafe fn get_mut<T>(&self, offset: usize, _guard: &Guard) -> *mut T {
        let cap = self.cap.get();
        assert_ne!(cap, 0);

        unsafe { self.ptr.get().add(offset) as *mut T }
    }

    pub fn reset(&self, _guard: &Guard) {
        self.len.store(0, Ordering::SeqCst);
    }
}

impl Drop for VecArena {
    fn drop(&mut self) {
        let ptr = self.ptr.get();
        let cap = self.cap.get();

        if cap != 0 {
            unsafe {
                let _ = Allcator::from_raw_parts(ptr as _, 0, cap / 8);
            }
        }
    }
}
