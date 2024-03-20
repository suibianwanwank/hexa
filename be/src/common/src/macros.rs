#[macro_export]
macro_rules! gen_index_type {
    ($index_type:ident) => {
        #[derive(Debug, Copy, Clone)]
        struct $index_type(usize);

        impl AddAssign for $index_type {
            fn add_assign(&mut self, rhs: Self) {
                self.0 += rhs.0
            }
        }

        impl $index_type {
            pub fn incr(& mut self) {
                self.0+=1;
            }
        }

        impl From<usize> for $index_type {
            fn from(value: usize) -> Self {
                Self(value)
            }
        }

        impl From<$index_type> for usize {
            fn from(value: $index_type) -> usize {
                value.0
            }
        }
    };
}
