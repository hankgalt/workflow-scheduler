package future

import (
	"context"
	"sync"
	"time"
)

type FuncReturning[A any, R any] interface {
	~func(A) R
}

type Future[F FuncReturning[A, R], A any, R any] interface {
	Get(ctx context.Context, arg A, valuePtr *R) error
	IsReady() bool
	Arg() A
}

type dummyFuture[F FuncReturning[A, R], A any, R any] struct {
	fn        F
	arg       A
	delay     time.Duration
	ready     chan struct{}
	readyFlag bool
	mu        sync.RWMutex
	once      sync.Once
	result    R
}

func (f *dummyFuture[F, A, R]) Get(ctx context.Context, arg A, valuePtr *R) error {
	f.once.Do(func() {
		go func() {
			time.Sleep(f.delay)

			res := f.fn(arg)
			f.mu.Lock()
			f.result = res
			f.readyFlag = true
			f.mu.Unlock()
			close(f.ready)
		}()
	})

	select {
	case <-f.ready:
		if valuePtr != nil {
			*valuePtr = f.result
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *dummyFuture[F, A, R]) IsReady() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.readyFlag
}

func (f *dummyFuture[F, A, R]) Arg() A {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.arg
}

func NewDummyFuture[F FuncReturning[A, R], A any, R any](delay time.Duration, fn F, arg A) Future[F, A, R] {
	f := &dummyFuture[F, A, R]{
		fn:    fn,
		arg:   arg,
		delay: delay,
		ready: make(chan struct{}),
	}
	return f
}
