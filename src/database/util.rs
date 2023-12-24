pub trait FromU8 {
    fn from_u8(data: &[u8]) -> Self;
}

macro_rules! impl_from_u8_for_int {
    ($T: ty, $N: expr) => {
        impl FromU8 for $T {
            fn from_u8(data: &[u8]) -> $T {
                assert_eq!(data.len(), $N);

                let mut value: $T = 0;

                for i in 0..$N {
                    value |= (data[i] as $T) << 8 * ($N - i - 1);
                }

                value
            }
        }
    };
}

impl_from_u8_for_int!(u8, 1);
impl_from_u8_for_int!(i8, 1);
impl_from_u8_for_int!(u16, 2);
impl_from_u8_for_int!(i16, 2);
impl_from_u8_for_int!(u32, 4);
impl_from_u8_for_int!(i32, 4);
impl_from_u8_for_int!(u64, 8);
impl_from_u8_for_int!(i64, 8);
impl_from_u8_for_int!(u128, 16);
impl_from_u8_for_int!(i128, 16);
