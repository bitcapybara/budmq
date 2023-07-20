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
