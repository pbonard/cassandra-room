package db

import (
	"errors"
	"fmt"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// requestError is an error reported to the client with a protocol error
// code (exceptions.RequestValidationException and friends).
type requestError struct {
	kind     errKind
	msg      string
	keyspace string
	table    string
}

type errKind uint8

const (
	errInvalid errKind = iota
	errSyntax
	errConfig
	errAlreadyExists
	errUnauthorized
)

func (e *requestError) Error() string { return e.msg }

func invalidf(format string, args ...any) error {
	return &requestError{kind: errInvalid, msg: fmt.Sprintf(format, args...)}
}

func syntaxf(format string, args ...any) error {
	return &requestError{kind: errSyntax, msg: fmt.Sprintf(format, args...)}
}

func configf(format string, args ...any) error {
	return &requestError{kind: errConfig, msg: fmt.Sprintf(format, args...)}
}

func alreadyExists(ks, table string) error {
	e := &requestError{kind: errAlreadyExists, keyspace: ks, table: table}
	if table == "" {
		e.msg = fmt.Sprintf("Keyspace %s already exists", ks)
	} else {
		e.msg = fmt.Sprintf("Table %s.%s already exists", ks, table)
	}
	return e
}

// ErrorMessage converts an error to the protocol's error message.
func ErrorMessage(err error) message.Message {
	var re *requestError
	var ue2 *unavailableError
	var te *timeoutError
	switch {
	case errors.As(err, &ue2):
		return &message.Unavailable{ErrorMessage: ue2.Error(), Consistency: ue2.cl, Required: int32(ue2.required), Alive: int32(ue2.alive)}
	case errors.As(err, &te):
		if te.write {
			wt := primitive.WriteTypeSimple
			if te.batch {
				wt = primitive.WriteTypeUnloggedBatch
			}
			return &message.WriteTimeout{ErrorMessage: te.Error(), Consistency: te.cl, Received: int32(te.received), BlockFor: int32(te.blockFor), WriteType: wt}
		}
		return &message.ReadTimeout{ErrorMessage: te.Error(), Consistency: te.cl, Received: int32(te.received), BlockFor: int32(te.blockFor)}
	}
	var se *cql.SyntaxError
	var ue *cql.UnsupportedError
	switch {
	case errors.As(err, &re):
		switch re.kind {
		case errSyntax:
			return &message.SyntaxError{ErrorMessage: re.msg}
		case errConfig:
			return &message.ConfigError{ErrorMessage: re.msg}
		case errAlreadyExists:
			return &message.AlreadyExists{ErrorMessage: re.msg, Keyspace: re.keyspace, Table: re.table}
		case errUnauthorized:
			return &message.Unauthorized{ErrorMessage: re.msg}
		}
		return &message.Invalid{ErrorMessage: re.msg}
	case errors.As(err, &se):
		return &message.SyntaxError{ErrorMessage: se.Error()}
	case errors.As(err, &ue):
		return &message.Invalid{ErrorMessage: ue.Error()}
	}
	return &message.ServerError{ErrorMessage: err.Error()}
}
