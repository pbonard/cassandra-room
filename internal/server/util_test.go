package server

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeNetError struct {
	timeout bool
}

func (e fakeNetError) Error() string   { return "fake net error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return false }

func TestIsTemporaryAcceptError(t *testing.T) {
	assert.True(t, isTemporaryAcceptError(syscall.EMFILE))
	assert.True(t, isTemporaryAcceptError(syscall.ENFILE))
	assert.True(t, isTemporaryAcceptError(syscall.ECONNABORTED))
	assert.True(t, isTemporaryAcceptError(syscall.EINTR))
	assert.True(t, isTemporaryAcceptError(fakeNetError{timeout: true}))
	assert.False(t, isTemporaryAcceptError(fakeNetError{timeout: false}))
	assert.False(t, isTemporaryAcceptError(errors.New("permanent error")))
}

func TestIsTimeoutError(t *testing.T) {
	assert.True(t, isTimeoutError(os.ErrDeadlineExceeded))
	assert.True(t, isTimeoutError(fakeNetError{timeout: true}))
	assert.False(t, isTimeoutError(fakeNetError{timeout: false}))
	assert.False(t, isTimeoutError(errors.New("other error")))
}
