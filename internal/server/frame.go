package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/go-cassandra-native-protocol/segment"
)

// validateFrameHeader checks that hdr has a supported protocol version, matches the
// session's negotiated version (if initialized), and does not exceed maxFrameSize.
func (s *Server) validateFrameHeader(session *Session, hdr *frame.Header) (errFrame *frame.Frame, bodyLenExceeded bool) {
	maxFrame := s.getMaxFrameSize()
	if hdr.BodyLength < 0 || int(hdr.BodyLength) > maxFrame {
		return frame.NewFrame(
			normalizedErrorVersion(hdr.Version),
			hdr.StreamId,
			&message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Frame body length %d exceeds maximum allowed frame size of %d bytes", hdr.BodyLength, maxFrame),
			},
		), true
	}
	if hdr.Version < primitive.ProtocolVersion3 || hdr.Version > primitive.ProtocolVersion5 {
		return frame.NewFrame(
			normalizedErrorVersion(session.protocolVersion()),
			hdr.StreamId,
			&message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d); supported versions are 3/v3, 4/v4, 5/v5", uint8(hdr.Version)),
			},
		), false
	}
	if negVer := session.protocolVersion(); negVer != 0 && hdr.Version != negVer {
		return frame.NewFrame(
			negVer,
			hdr.StreamId,
			&message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Invalid message version (%d); negotiated protocol version for this connection is %d", uint8(hdr.Version), uint8(negVer)),
			},
		), false
	}
	return nil, false
}

func (s *Server) readAndDispatchFrame(
	conn net.Conn,
	connCtx context.Context,
	session *Session,
	reader io.Reader,
	peekStreamID int16,
	maxAvailableBody int,
	preReservedBytes int64,
	inFlight *sync.WaitGroup,
	sem chan struct{},
) error {
	releaseReserved := func(n int64) {
		if n > 0 {
			s.admission.releaseBytes(n)
		}
	}

	hdr, err := s.codec.DecodeHeader(reader)
	if err != nil {
		releaseReserved(preReservedBytes)
		var protoErr *frame.ProtocolVersionErr
		if errors.As(err, &protoErr) || maxAvailableBody >= 0 {
			resp := frame.NewFrame(
				normalizedErrorVersion(session.protocolVersion()),
				peekStreamID,
				&message.ProtocolError{ErrorMessage: err.Error()},
			)
			_ = s.writeFrame(conn, session, resp)
		}
		return err
	}

	if maxAvailableBody >= 0 && int(hdr.BodyLength) > maxAvailableBody {
		releaseReserved(preReservedBytes)
		resp := frame.NewFrame(
			normalizedErrorVersion(hdr.Version),
			hdr.StreamId,
			&message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Frame body length %d exceeds remaining segment payload (%d bytes)", hdr.BodyLength, maxAvailableBody),
			},
		)
		_ = s.writeFrame(conn, session, resp)
		return fmt.Errorf("frame body length %d exceeds remaining segment payload %d", hdr.BodyLength, maxAvailableBody)
	}

	if errResp, bodyLenExceeded := s.validateFrameHeader(session, hdr); errResp != nil {
		releaseReserved(preReservedBytes)
		if bodyLenExceeded {
			_ = s.writeFrame(conn, session, errResp)
			return fmt.Errorf("frame body length %d exceeds max frame size", hdr.BodyLength)
		}
		if err := s.codec.DiscardBody(hdr, reader); err != nil {
			return err
		}
		return s.writeFrame(conn, session, errResp)
	}

	isControl := hdr.OpCode == primitive.OpCodeOptions ||
		hdr.OpCode == primitive.OpCodeStartup ||
		hdr.OpCode == primitive.OpCodeRegister
	bodyBytes := preReservedBytes

	if !isControl && hdr.BodyLength > 0 && bodyBytes == 0 {
		reqBytes := int64(hdr.BodyLength)
		overloaded, err := s.admission.acquireBytes(connCtx, reqBytes, session.getThrowOnOverload())
		if err != nil {
			return err
		}
		if overloaded {
			if err := s.codec.DiscardBody(hdr, reader); err != nil {
				return err
			}
			resp := frame.NewFrame(
				hdr.Version,
				hdr.StreamId,
				&message.Overloaded{ErrorMessage: "server is overloaded (in-flight byte limit reached)"},
			)
			return s.writeFrame(conn, session, resp)
		}
		bodyBytes = reqBytes
	}

	rawBody, err := s.codec.DecodeRawBody(hdr, reader)
	if err != nil {
		if !isControl && bodyBytes > 0 {
			s.admission.releaseBytes(bodyBytes)
		}
		return err
	}

	// If the frame is compressed, validate its uncompressed length header and reserve
	// any additional decompressed bytes against the global in-flight byte budget before
	// decompressing or spawning a worker goroutine.
	var claimedUncompressedLen int
	if hdr.Flags.Contains(primitive.HeaderFlagCompressed) {
		if session.isModernLayout() {
			if !isControl && bodyBytes > 0 {
				s.admission.releaseBytes(bodyBytes)
			}
			resp := frame.NewFrame(
				hdr.Version,
				hdr.StreamId,
				&message.ProtocolError{ErrorMessage: "frames inside protocol v5 segments must not have HeaderFlagCompressed set"},
			)
			return s.writeFrame(conn, session, resp)
		}
		if hdr.OpCode == primitive.OpCodeOptions || hdr.OpCode == primitive.OpCodeStartup {
			if !isControl && bodyBytes > 0 {
				s.admission.releaseBytes(bodyBytes)
			}
			resp := frame.NewFrame(
				hdr.Version,
				hdr.StreamId,
				&message.ProtocolError{ErrorMessage: "OPTIONS and STARTUP frames must not be compressed"},
			)
			return s.writeFrame(conn, session, resp)
		}
		algo, comp := session.compression()
		if comp == nil {
			if !isControl && bodyBytes > 0 {
				s.admission.releaseBytes(bodyBytes)
			}
			resp := frame.NewFrame(
				hdr.Version,
				hdr.StreamId,
				&message.ProtocolError{ErrorMessage: "received compressed frame before compression was negotiated in STARTUP"},
			)
			return s.writeFrame(conn, session, resp)
		}
		uLen, err := validateCompressedBodyBounds(algo, rawBody, s.getMaxFrameSize())
		if err != nil {
			if !isControl && bodyBytes > 0 {
				s.admission.releaseBytes(bodyBytes)
			}
			resp := frame.NewFrame(
				hdr.Version,
				hdr.StreamId,
				&message.ProtocolError{ErrorMessage: err.Error()},
			)
			return s.writeFrame(conn, session, resp)
		}
		claimedUncompressedLen = uLen
		if !isControl && int64(uLen) > bodyBytes {
			// Release the compressed-size reservation first and acquire the full
			// decompressed size in a single call so the activeBytes == 0 exception
			// for single frames larger than maxInFlightBytes cannot deadlock against
			// our own initial reservation.
			if bodyBytes > 0 {
				s.admission.releaseBytes(bodyBytes)
				bodyBytes = 0
			}
			overloaded, err := s.admission.acquireBytes(connCtx, int64(uLen), session.getThrowOnOverload())
			if err != nil {
				return err
			}
			if overloaded {
				resp := frame.NewFrame(
					hdr.Version,
					hdr.StreamId,
					&message.Overloaded{ErrorMessage: "server is overloaded (in-flight byte limit reached)"},
				)
				return s.writeFrame(conn, session, resp)
			}
			bodyBytes = int64(uLen)
		}
	}

	rawFrame := &frame.RawFrame{
		Header: hdr,
		Body:   rawBody,
	}

	// Connection/session-level statements (`USE`, `STARTUP`, `OPTIONS`, `REGISTER`)
	// wait for prior in-flight frames on this connection so a `USE ks` statement
	// deterministically takes effect before subsequent frames read from the wire.
	isBarrier, barrierErr := s.isBarrierRawFrame(session, rawFrame, claimedUncompressedLen)
	if barrierErr != nil {
		if !isControl && bodyBytes > 0 {
			s.admission.releaseBytes(bodyBytes)
		}
		resp := frame.NewFrame(
			hdr.Version,
			hdr.StreamId,
			&message.ProtocolError{ErrorMessage: barrierErr.Error()},
		)
		return s.writeFrame(conn, session, resp)
	}
	if isBarrier {
		inFlight.Wait()
		respFrame := s.handleRawFrame(connCtx, session, rawFrame)
		if !isControl && bodyBytes > 0 {
			s.admission.releaseBytes(bodyBytes)
		}
		return s.writeFrame(conn, session, respFrame)
	}

	select {
	case sem <- struct{}{}:
	case <-connCtx.Done():
		if !isControl && bodyBytes > 0 {
			s.admission.releaseBytes(bodyBytes)
		}
		return connCtx.Err()
	}

	inFlight.Add(1)
	go func(rf *frame.RawFrame, releaseByteCount int64, control bool) {
		defer func() {
			if !control && releaseByteCount > 0 {
				s.admission.releaseBytes(releaseByteCount)
			}
			<-sem
			inFlight.Done()
		}()
		respFrame := s.handleRawFrame(connCtx, session, rf)
		_ = s.writeFrame(conn, session, respFrame)
	}(rawFrame, bodyBytes, isControl)
	return nil
}

// writeFrame serializes (and optionally compresses or wraps in a v5 segment) a response
// frame f and flushes it to conn in a single buffered write under session.writeMu.
func (s *Server) writeFrame(conn net.Conn, session *Session, f *frame.Frame) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()

	if wt := s.getWriteTimeout(); wt > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(wt))
	}
	bw := bufioWriterPool.Get().(*bufio.Writer)
	bw.Reset(conn)

	var err error
	if session.isModernLayout() {
		// In Protocol v5 segment mode, frames are never compressed individually;
		// compression is applied to the enclosing segment payload.
		f.Header.Flags = f.Header.Flags.Remove(primitive.HeaderFlagCompressed)
		var frameBuf bytes.Buffer
		err = s.codec.EncodeFrame(f, &frameBuf)
		if err == nil {
			segCodec := session.segmentCodec()
			rawBytes := frameBuf.Bytes()
			if len(rawBytes) <= segment.MaxPayloadLength {
				seg := &segment.Segment{
					Header:  &segment.Header{IsSelfContained: true},
					Payload: &segment.Payload{UncompressedData: rawBytes},
				}
				err = segCodec.EncodeSegment(seg, bw)
			} else {
				for len(rawBytes) > 0 {
					chunkLen := len(rawBytes)
					if chunkLen > segment.MaxPayloadLength {
						chunkLen = segment.MaxPayloadLength
					}
					seg := &segment.Segment{
						Header:  &segment.Header{IsSelfContained: false},
						Payload: &segment.Payload{UncompressedData: rawBytes[:chunkLen]},
					}
					if err = segCodec.EncodeSegment(seg, bw); err != nil {
						break
					}
					rawBytes = rawBytes[chunkLen:]
				}
			}
		}
	} else {
		if _, comp := session.compression(); comp != nil {
			rawResp, convErr := s.codec.ConvertToRawFrame(f)
			if convErr != nil {
				err = convErr
			} else {
				if len(rawResp.Body) > 0 {
					compressedBody, compErr := comp.CompressFrame(rawResp.Body)
					if compErr != nil {
						err = compErr
					} else {
						rawResp.Body = compressedBody
						rawResp.Header.BodyLength = int32(len(compressedBody))
						rawResp.Header.Flags = rawResp.Header.Flags.Add(primitive.HeaderFlagCompressed)
					}
				}
				if err == nil {
					err = s.codec.EncodeRawFrame(rawResp, bw)
				}
			}
		} else {
			err = s.codec.EncodeFrame(f, bw)
		}
		if err == nil && f.Header.Version.SupportsModernFramingLayout() {
			session.consumePendingModernLayout()
		}
	}

	if err == nil {
		err = bw.Flush()
	}
	bw.Reset(nil)
	bufioWriterPool.Put(bw)

	if err != nil {
		if session.writeErr == nil {
			session.writeErr = fmt.Errorf("encode response frame: %w", err)
		}
		_ = conn.Close()
		return err
	}
	return nil
}

// handleRawFrame decompresses (if negotiated), validates inner message length bounds,
// converts the raw frame into a decoded Frame on the worker goroutine, and dispatches
// it through handleFrameWithContext.
func (s *Server) handleRawFrame(ctx context.Context, session *Session, rawFrame *frame.RawFrame) *frame.Frame {
	if err := s.decompressRawFrameInPlace(session, rawFrame); err != nil {
		return frame.NewFrame(
			normalizedErrorVersion(rawFrame.Header.Version),
			rawFrame.Header.StreamId,
			&message.ProtocolError{ErrorMessage: err.Error()},
		)
	}
	if err := validateRawBodyBounds(rawFrame.Header, rawFrame.Body); err != nil {
		return frame.NewFrame(
			normalizedErrorVersion(rawFrame.Header.Version),
			rawFrame.Header.StreamId,
			&message.ProtocolError{ErrorMessage: fmt.Sprintf("malformed frame body: %v", err)},
		)
	}
	decoded, err := s.codec.ConvertFromRawFrame(rawFrame)
	if err != nil {
		return frame.NewFrame(
			normalizedErrorVersion(rawFrame.Header.Version),
			rawFrame.Header.StreamId,
			&message.ProtocolError{ErrorMessage: fmt.Sprintf("failed to decode frame body: %v", err)},
		)
	}
	return s.handleFrameWithContext(ctx, session, decoded)
}

// isBarrierRawFrame returns true for control frames or USE statements (including
// QUERY with leading comments or EXECUTE of a prepared USE) that mutate
// connection-wide session state. If decompression of a small candidate frame fails,
// it returns an error so the caller can reply with ProtocolError immediately without
// decompressing a second time on a worker goroutine.
func (s *Server) isBarrierRawFrame(session *Session, rf *frame.RawFrame, claimedUncompressedLen int) (bool, error) {
	if rf == nil || rf.Header == nil {
		return false, nil
	}
	switch rf.Header.OpCode {
	case primitive.OpCodeOptions, primitive.OpCodeStartup, primitive.OpCodeRegister:
		return true, nil
	case primitive.OpCodeQuery:
		if rf.Header.Flags.Contains(primitive.HeaderFlagCompressed) {
			// Trade-off: skip reader-loop decompression for compressed QUERY frames over
			// 512 uncompressed bytes so large queries are decompressed on worker goroutines.
			// A synthetic USE statement padded past 512 bytes (e.g. via huge comments or
			// custom payloads) will not act as an in-connection barrier, which is acceptable
			// because real CQL drivers never send oversized USE frames.
			if claimedUncompressedLen > 512 {
				return false, nil
			}
			if err := s.decompressRawFrameInPlace(session, rf); err != nil {
				return false, err
			}
		}
	case primitive.OpCodeExecute:
		if rf.Header.Flags.Contains(primitive.HeaderFlagCompressed) {
			if err := s.decompressRawFrameInPlace(session, rf); err != nil {
				return false, err
			}
		}
	default:
		return false, nil
	}

	pos := 0
	if rf.Header.Flags.Contains(primitive.HeaderFlagCustomPayload) {
		var err error
		pos, err = skipBytesMap(rf.Body, pos)
		if err != nil {
			return false, nil
		}
	}

	switch rf.Header.OpCode {
	case primitive.OpCodeQuery:
		if pos+4 > len(rf.Body) {
			return false, nil
		}
		qLen := int(int32(binary.BigEndian.Uint32(rf.Body[pos : pos+4])))
		pos += 4
		if qLen <= 0 || qLen > len(rf.Body)-pos {
			return false, nil
		}
		query := string(rf.Body[pos : pos+qLen])
		// Fast zero-allocation pre-check: skip whitespace and comments and check
		// whether the first keyword is "USE" before preparing it on the reader goroutine.
		if !hasLeadingKeywordUse(query) {
			return false, nil
		}
		p, err := s.statement(session.Keyspace(), query)
		return err == nil && p.IsUse(), nil
	case primitive.OpCodeExecute:
		if pos+2 > len(rf.Body) {
			return false, nil
		}
		idLen := int(binary.BigEndian.Uint16(rf.Body[pos : pos+2]))
		pos += 2
		if idLen <= 0 || idLen > len(rf.Body)-pos {
			return false, nil
		}
		prep, ok := s.lookupPrepared(rf.Body[pos : pos+idLen])
		return ok && prep.IsUse(), nil
	default:
		return false, nil
	}
}

// decompressRawFrameInPlace safely validates the uncompressed length header and
// decompresses rawFrame.Body if HeaderFlagCompressed is set, clearing the flag
// so subsequent validation and decoding operate on the plain uncompressed bytes.
func (s *Server) decompressRawFrameInPlace(session *Session, rawFrame *frame.RawFrame) error {
	if rawFrame == nil || rawFrame.Header == nil {
		return nil
	}
	if !rawFrame.Header.Flags.Contains(primitive.HeaderFlagCompressed) {
		return nil
	}
	if rawFrame.Header.OpCode == primitive.OpCodeOptions || rawFrame.Header.OpCode == primitive.OpCodeStartup {
		return fmt.Errorf("OPTIONS and STARTUP frames must not be compressed")
	}
	algo, comp := session.compression()
	if comp == nil {
		return fmt.Errorf("received compressed frame before compression was negotiated in STARTUP")
	}
	if _, err := validateCompressedBodyBounds(algo, rawFrame.Body, s.getMaxFrameSize()); err != nil {
		return err
	}
	decompressed, err := comp.DecompressFrame(rawFrame.Body)
	if err != nil {
		return fmt.Errorf("failed to decompress %s frame body: %w", algo, err)
	}
	rawFrame.Body = decompressed
	rawFrame.Header.BodyLength = int32(len(decompressed))
	rawFrame.Header.Flags = rawFrame.Header.Flags.Remove(primitive.HeaderFlagCompressed)
	return nil
}

// handleFrameWithContext processes a single incoming request frame with ctx, enforcing
// global admission control and returning the response frame.
func (s *Server) handleFrameWithContext(ctx context.Context, session *Session, req *frame.Frame) (resp *frame.Frame) {
	defer func() {
		if r := recover(); r != nil {
			resp = frame.NewFrame(
				req.Header.Version,
				req.Header.StreamId,
				&message.ServerError{ErrorMessage: fmt.Sprintf("internal server panic: %v", r)},
			)
		}
	}()

	if ctx == nil {
		ctx = s.serverCtx
	}

	if errResp, _ := s.validateFrameHeader(session, req.Header); errResp != nil {
		return errResp
	}

	// Global admission control: gate query/prepare/execute/batch work (not lightweight
	// connection handshake/control frames like OPTIONS, STARTUP, or REGISTER).
	// If THROW_ON_OVERLOAD=1 was set on STARTUP, fail fast with ERROR OVERLOADED
	// when global concurrency is saturated; otherwise apply backpressure.
	switch req.Body.Message.(type) {
	case *message.Options, *message.Startup, *message.Register:
		// Handshake/control frames bypass query admission control.
	default:
		throwOnOverload := session != nil && session.getThrowOnOverload()
		overloaded, err := s.admission.acquireRequest(ctx, throwOnOverload)
		if err != nil {
			return frame.NewFrame(
				req.Header.Version,
				req.Header.StreamId,
				&message.ServerError{ErrorMessage: fmt.Sprintf("request canceled: %v", err)},
			)
		}
		if overloaded {
			return frame.NewFrame(
				req.Header.Version,
				req.Header.StreamId,
				&message.Overloaded{ErrorMessage: "server is overloaded (admission control limit reached)"},
			)
		}
		defer s.admission.releaseRequest()
	}

	respMsg := s.handleMessage(ctx, session, req.Header.Version, req.Body.Message)
	return frame.NewFrame(req.Header.Version, req.Header.StreamId, respMsg)
}
