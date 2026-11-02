package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/db"
	"github.com/datastax/go-cassandra-native-protocol/compression/lz4"
	"github.com/datastax/go-cassandra-native-protocol/compression/snappy"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (s *Server) handleMessage(
	ctx context.Context,
	session *Session,
	version primitive.ProtocolVersion,
	msg message.Message,
) message.Message {
	switch msg.(type) {
	case *message.Options, *message.Startup:
	default:
		if session != nil && !session.isInitialized() {
			return &message.ProtocolError{
				ErrorMessage: "Connection is not initialized; STARTUP must be sent before any other request",
			}
		}
	}

	switch m := msg.(type) {
	case *message.Options:
		compOpts := []string{"lz4", "snappy"}
		if version >= primitive.ProtocolVersion5 {
			compOpts = []string{"lz4"}
		}
		return &message.Supported{
			Options: map[string][]string{
				message.StartupOptionCqlVersion:  {"3.4.7"},
				message.StartupOptionCompression: compOpts,
				message.SupportedProtocolVersions: {
					"3/v3",
					"4/v4",
					"5/v5",
				},
			},
		}

	case *message.Startup:
		if session != nil && session.isInitialized() {
			return &message.ProtocolError{
				ErrorMessage: "Connection is already initialized; STARTUP may only be sent once per connection",
			}
		}
		if m.Options != nil {
			if rawComp, ok := m.Options[message.StartupOptionCompression]; ok {
				compAlgo := strings.ToLower(strings.TrimSpace(rawComp))
				switch compAlgo {
				case "":
					if session != nil {
						session.setCompression("", nil)
					}
				case "lz4":
					if session != nil {
						session.setCompression("lz4", lz4.Compressor{})
					}
				case "snappy":
					if version >= primitive.ProtocolVersion5 {
						return &message.ProtocolError{
							ErrorMessage: "snappy compression is not supported in protocol v5; supported algorithm is lz4",
						}
					}
					if session != nil {
						session.setCompression("snappy", snappy.Compressor{})
					}
				default:
					return &message.ProtocolError{
						ErrorMessage: fmt.Sprintf("Unsupported compression algorithm %q; supported algorithms are lz4, snappy", rawComp),
					}
				}
			}
			if session != nil && m.Options[message.StartupOptionThrowOnOverload] == "1" {
				session.setThrowOnOverload(true)
			}
		}
		if session != nil {
			session.setInitializedWithVersion(version)
			if version.SupportsModernFramingLayout() {
				session.setPendingModernLayout(true)
			}
		}
		if s.db.AuthEnabled() {
			return &message.Authenticate{Authenticator: "org.apache.cassandra.auth.PasswordAuthenticator"}
		}
		return &message.Ready{}

	case *message.AuthResponse:
		if !s.db.AuthEnabled() {
			return &message.ProtocolError{ErrorMessage: "Unexpected AUTH_RESPONSE: authentication is not enabled"}
		}
		// SASL PLAIN: [authzid] NUL authcid NUL password.
		parts := strings.Split(string(m.Token), "\x00")
		if len(parts) != 3 {
			return &message.AuthenticationError{ErrorMessage: "Authentication failed: malformed PLAIN token"}
		}
		if err := s.db.Authenticate(parts[1], parts[2]); err != nil {
			return &message.AuthenticationError{ErrorMessage: err.Error()}
		}
		if session != nil {
			session.setUser(parts[1])
		}
		return &message.AuthSuccess{}
	}

	if s.db.AuthEnabled() && session != nil && session.User() == "" {
		return &message.Unauthorized{ErrorMessage: "You have not logged in"}
	}

	switch m := msg.(type) {
	case *message.Register:
		return &message.Ready{}

	case *message.Query:
		ks := session.Keyspace()
		if m.Options != nil && m.Options.Keyspace != "" {
			ks = m.Options.Keyspace
		}
		p, err := s.statement(ks, m.Query)
		if err != nil {
			return db.ErrorMessage(err)
		}
		return s.execute(ctx, session, p, db.OptionsOf(m.Options, version))

	case *message.Prepare:
		ks := session.Keyspace()
		if m.Keyspace != "" {
			ks = m.Keyspace
		}
		p, err := s.statement(ks, m.Query)
		if err != nil {
			return db.ErrorMessage(err)
		}
		vars, result, resultID := p.Metadata()
		return &message.PreparedResult{
			PreparedQueryId:   p.ID,
			ResultMetadataId:  resultID,
			VariablesMetadata: vars,
			ResultMetadata:    result,
		}

	case *message.Execute:
		p, ok := s.lookupPrepared(m.QueryId)
		if !ok {
			return &message.Unprepared{
				Id:           m.QueryId,
				ErrorMessage: fmt.Sprintf("unprepared query id %x", m.QueryId),
			}
		}
		o := db.OptionsOf(m.Options, version)
		if version >= primitive.ProtocolVersion5 {
			o.ResultMetadataID = m.ResultMetadataId
		}
		return s.execute(ctx, session, p, o)

	case *message.Batch:
		ks := session.Keyspace()
		if m.Keyspace != "" {
			ks = m.Keyspace
		}
		children := make([]db.BatchChild, len(m.Children))
		for i, child := range m.Children {
			if len(child.Id) > 0 {
				p, ok := s.lookupPrepared(child.Id)
				if !ok {
					return &message.Unprepared{
						Id:           child.Id,
						ErrorMessage: fmt.Sprintf("unprepared batch child id %x", child.Id),
					}
				}
				children[i] = db.BatchChild{Prepared: p, Values: child.Values}
				continue
			}
			p, err := s.statement(ks, child.Query)
			if err != nil {
				return db.ErrorMessage(err)
			}
			children[i] = db.BatchChild{Prepared: p, Values: child.Values}
		}
		if s.testHook != nil {
			if err := s.testHook(ctx, "BATCH"); err != nil {
				return &message.ServerError{ErrorMessage: err.Error()}
			}
		}
		o := &db.RequestOptions{Consistency: m.Consistency,
			Timestamp: m.DefaultTimestamp, NowInSec: m.NowInSeconds, Version: version, User: session.User(),
		}
		if m.SerialConsistency != nil {
			o.SerialConsistency = *m.SerialConsistency
		}
		o.Trace, _ = ctx.Value(traceKey{}).(*db.Trace)
		return s.db.ExecuteBatch(m.Type, children, o)

	default:
		return &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("unsupported request opcode: %v", msg.GetOpCode()),
		}
	}
}

// execute runs a statement and applies a USE to the session.
func (s *Server) execute(ctx context.Context, session *Session, p *db.Prepared, o *db.RequestOptions) message.Message {
	if s.testHook != nil {
		if err := s.testHook(ctx, p.Query); err != nil {
			return &message.ServerError{ErrorMessage: err.Error()}
		}
	}
	if session != nil {
		o.User = session.User()
	}
	o.Trace, _ = ctx.Value(traceKey{}).(*db.Trace)
	res := s.db.Execute(p, o)
	if sk, ok := res.(*message.SetKeyspaceResult); ok && session != nil {
		session.setKeyspace(sk.Keyspace)
	}
	return res
}

// traceKey carries a traced request's trace in its context.
type traceKey struct{}
