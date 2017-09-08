package canceler

import (
	"context"
	"errors"
)

// ErrCanceled can be used by clients to signal that
// work has been stopped.
var ErrCanceled = errors.New("closed")

// A Canceler is a primitive for implementing graceful shutdown.
type Canceler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// New builds a new Canceler.
func New() *Canceler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Canceler{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Cancel signals that work should be stopped. It does not wait
// for the work to stop.
//
// After the first call, subsequent calls do nothing.
func (c Canceler) Cancel() {
	c.cancel()
}

// Done returns a channel that's closed when work should be canceled.
func (c Canceler) Done() <-chan struct{} {
	return c.ctx.Done()
}
