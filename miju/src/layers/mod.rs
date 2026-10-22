use miju_core::{backend::Backend, worker::builder::WorkerBuilder};
use tower::layer::util::{Identity, Stack};

/// An extension trait that extends `WorkerBuilder` with additional middleware methods.
///
/// Layers added earlier wrap those added later. Requests pass through them in
/// insertion order; responses and errors pass back through them in reverse order.
pub trait WorkerBuilderExt<Args, Source, Middleware> {
    /// Optionally adds a new layer `T` into the [`WorkerBuilder`].
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::Either<T, Identity>, Middleware>>;

    /// Adds a [`Layer`] built from a function that accepts a service and returns another service.
    ///
    /// [`Layer`]: tower::Layer
    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::layer::LayerFn<F>, Middleware>>;

    /// Maps one request type to another.
    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone;

    /// Maps one response type to another.
    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>>;

    /// Maps one error type to another.
    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapErrLayer<F>, Middleware>>;

    /// Composes a function that transforms futures produced by the service.
    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>>;

    /// Applies an asynchronous function after the service, regardless of whether the future succeeds or fails.
    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::ThenLayer<F>, Middleware>>;

    /// Executes a new future after this service's future resolves.
    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::AndThenLayer<F>, Middleware>>;

    /// Maps the service's result type to a different value, regardless of success or failure.
    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapResultLayer<F>, Middleware>>;
}

impl<Args, Source, Middleware> WorkerBuilderExt<Args, Source, Middleware>
    for WorkerBuilder<Args, Source, Middleware>
where
    Source: Backend,
{
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::Either<T, Identity>, Middleware>> {
        self.layer(tower::util::option_layer(layer))
    }

    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::layer::LayerFn<F>, Middleware>> {
        self.layer(tower::layer::layer_fn(f))
    }

    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone,
    {
        self.layer(tower::util::MapRequestLayer::new(f))
    }

    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>> {
        self.layer(tower::util::MapResponseLayer::new(f))
    }

    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapErrLayer<F>, Middleware>> {
        self.layer(tower::util::MapErrLayer::new(f))
    }

    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>> {
        self.layer(tower::util::MapFutureLayer::new(f))
    }

    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::ThenLayer<F>, Middleware>> {
        self.layer(tower::util::ThenLayer::new(f))
    }

    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::AndThenLayer<F>, Middleware>> {
        self.layer(tower::util::AndThenLayer::new(f))
    }

    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Source, Stack<tower::util::MapResultLayer<F>, Middleware>> {
        self.layer(tower::util::MapResultLayer::new(f))
    }
}
