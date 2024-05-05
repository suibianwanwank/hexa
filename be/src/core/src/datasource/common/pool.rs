// use std::future::Future;
// use std::sync::Arc;
// use std::time::Duration;
// use moka::future::{Cache, CacheBuilder};
//
// pub struct MultiSourceConnectionPool<T> {
//     cache: Cache<Arc<str>, T>,
// }
//
// impl<T> MultiSourceConnectionPool<T> {
//     pub fn with_capacity(capacity: usize) -> Self {
//         MultiSourceConnectionPool {
//             cache:Cache::builder()
//                 .max_capacity(capacity as u64)
//                 .time_to_live(Duration::from_secs(60 * 24))
//                 .build()
//         }
//     }
//
//
//     pub fn get_connection_pool<F, Fut>(&self, name: &str, create_connection: F) -> &T
//         where
//             F: FnOnce() -> Fut,
//             Fut: Future<Output = T>,
//     {
//         let a= Cache::builder()
//             .max_capacity(capacity as u64)
//             .time_to_live(Duration::from_secs(60 * 24))
//             .build();
//         a.get_with(name, create_connection)
//     }
// }
//
//
//
