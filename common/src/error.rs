pub trait WrapError<T, E>
where
    E: std::error::Error,
{
    fn wrap<C>(self, context: C) -> std::result::Result<T, E>
    where
        C: std::fmt::Display + Send + Sync + 'static;

    fn with_wrap<F, C>(self, context: F) -> std::result::Result<T, E>
    where
        F: FnOnce() -> C,
        C: std::fmt::Display + Send + Sync + 'static;
}

#[macro_export]
macro_rules! wrap_error_impl {
    ($e: path, $var: path) => {
        impl<T> WrapError<T, Error> for std::result::Result<T, $e> {
            fn wrap<C>(self, context: C) -> std::result::Result<T, Error>
            where
                C: std::fmt::Display + Send + Sync + 'static,
            {
                self.map_err(|e| $var(format!("{e}: {context}")))
            }

            fn with_wrap<F, C>(self, context: F) -> std::result::Result<T, Error>
            where
                F: FnOnce() -> C,
                C: std::fmt::Display + Send + Sync + 'static,
            {
                self.map_err(|e| $var(format!("{e}: {}", context())))
            }
        }
    };
}
