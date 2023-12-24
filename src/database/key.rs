use super::error::Error;

pub trait IntoLevelDBKey {
    fn as_u8_slice_for_write(&self, f: &dyn Fn(&[u8]) -> Result<(), Error>) -> Result<(), Error>;
    fn as_u8_slice_for_get(
        &self,
        f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Option<Vec<u8>>, Error>;
}

impl IntoLevelDBKey for &[u8] {
    fn as_u8_slice_for_write(&self, f: &dyn Fn(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        f(self)
    }

    fn as_u8_slice_for_get(
        &self,
        f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Option<Vec<u8>>, Error> {
        f(self)
    }
}

impl IntoLevelDBKey for &str {
    fn as_u8_slice_for_write(&self, f: &dyn Fn(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        f(self.as_bytes())
    }

    fn as_u8_slice_for_get(
        &self,
        f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Option<Vec<u8>>, Error> {
        f(self.as_bytes())
    }
}

impl IntoLevelDBKey for String {
    fn as_u8_slice_for_write(&self, f: &dyn Fn(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        f(self.as_bytes())
    }

    fn as_u8_slice_for_get(
        &self,
        f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Option<Vec<u8>>, Error> {
        f(self.as_bytes())
    }
}

impl IntoLevelDBKey for Vec<u8> {
    fn as_u8_slice_for_write(&self, f: &dyn Fn(&[u8]) -> Result<(), Error>) -> Result<(), Error> {
        f(self.as_slice())
    }

    fn as_u8_slice_for_get(
        &self,
        f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
    ) -> Result<Option<Vec<u8>>, Error> {
        f(self.as_slice())
    }
}

macro_rules! impl_into_level_db_key_for_integer {
    ($T: ty) => {
        impl IntoLevelDBKey for $T {
            fn as_u8_slice_for_write(
                &self,
                f: &dyn Fn(&[u8]) -> Result<(), Error>,
            ) -> Result<(), Error> {
                f(&self.to_be_bytes()[..])
            }

            fn as_u8_slice_for_get(
                &self,
                f: &dyn Fn(&[u8]) -> Result<Option<Vec<u8>>, Error>,
            ) -> Result<Option<Vec<u8>>, Error> {
                f(&self.to_be_bytes()[..])
            }
        }
    };
}

impl_into_level_db_key_for_integer!(i8);
impl_into_level_db_key_for_integer!(i16);
impl_into_level_db_key_for_integer!(i32);
impl_into_level_db_key_for_integer!(i64);
impl_into_level_db_key_for_integer!(i128);
impl_into_level_db_key_for_integer!(u8);
impl_into_level_db_key_for_integer!(u16);
impl_into_level_db_key_for_integer!(u32);
impl_into_level_db_key_for_integer!(u64);
impl_into_level_db_key_for_integer!(u128);
