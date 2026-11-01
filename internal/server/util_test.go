package server

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestResolveKeyspaceAndExtractStatementMeta(t *testing.T) {
	sess := &Session{}
	sess.setKeyspace("sess_ks")

	assert.Equal(t, "explicit_ks", sess.resolveKeyspace("override_ks", "explicit_ks"))
	assert.Equal(t, "override_ks", sess.resolveKeyspace("override_ks", ""))
	assert.Equal(t, "sess_ks", sess.resolveKeyspace("", ""))
	assert.Equal(t, "", (*Session)(nil).resolveKeyspace("", ""))

	sel := &cql.SelectStatement{
		Keyspace:  "",
		Table:     "riders",
		BindNames: []string{"id", "name"},
	}
	ks, tbl, binds := sess.extractStatementMeta("", sel)
	assert.Equal(t, "sess_ks", ks)
	assert.Equal(t, "riders", tbl)
	assert.Equal(t, []string{"id", "name"}, binds)

	require.NoError(t, sess.validateStatementKeyspace("", sel))
	require.Error(t, (&Session{}).validateStatementKeyspace("", sel))
}

func TestResolveValues(t *testing.T) {
	v1 := primitive.NewValue([]byte("1"))
	v2 := primitive.NewValue([]byte("2"))

	// No bind names, nil options
	vals, err := resolveValues(nil, nil)
	require.NoError(t, err)
	assert.Nil(t, vals)

	// Missing required bind values when options == nil
	_, err = resolveValues(nil, []string{""})
	require.Error(t, err)

	// Named values reordered to match bindNames
	vals, err = resolveValues(&message.QueryOptions{
		NamedValues: map[string]*primitive.Value{
			"b": v2,
			"a": v1,
		},
	}, []string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, []*primitive.Value{v1, v2}, vals)

	// Positional values
	vals, err = resolveValues(&message.QueryOptions{
		PositionalValues: []*primitive.Value{v1, v2},
	}, []string{"", ""})
	require.NoError(t, err)
	assert.Equal(t, []*primitive.Value{v1, v2}, vals)
}
