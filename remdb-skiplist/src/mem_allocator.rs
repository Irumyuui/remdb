use std::alloc::Layout;

mod block_arena;
mod defalut_allocator;

pub mod prelude {
    #![allow(unused)]

    pub use super::MemAllocator;
    pub use super::block_arena::BlockArena;
    pub use super::defalut_allocator::DefaultAllocator;
}

pub trait MemAllocator: Send + Sync {
    /// # Safety
    ///
    /// This function should not be called before the horsemen are ready.
    unsafe fn allocate(&self, layout: Layout) -> *mut u8;

    fn mem_usage(&self) -> usize;
}
