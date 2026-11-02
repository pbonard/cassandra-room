package server

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func normalizedErrorVersion(v primitive.ProtocolVersion) primitive.ProtocolVersion {
	if v >= primitive.ProtocolVersion3 && v <= primitive.ProtocolVersion5 {
		return v
	}
	return primitive.ProtocolVersion4
}

// extractPeekedStreamID extracts the client's StreamId from the first 4 bytes of a
// frame header even when the protocol version is unsupported by the codec.
func extractPeekedStreamID(hdrPeek []byte) int16 {
	if len(hdrPeek) < 3 {
		return 0
	}
	ver := hdrPeek[0] & 0x7F
	if ver == 1 || ver == 2 {
		return int16(int8(hdrPeek[2]))
	}
	if len(hdrPeek) >= 4 {
		return int16(binary.BigEndian.Uint16(hdrPeek[2:4]))
	}
	return 0
}

// hasLeadingKeywordUse scans q without allocating, skipping ASCII whitespace,
// '--' and '//' line comments, and '/* ... */' block comments, and returns true
// if and only if the first token is the keyword "USE" (case-insensitive).
func hasLeadingKeywordUse(q string) bool {
	n := len(q)
	i := 0
	for i < n {
		c := q[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if c == '-' && i+1 < n && q[i+1] == '-' {
			i += 2
			for i < n && q[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < n {
			if q[i+1] == '/' {
				i += 2
				for i < n && q[i] != '\n' {
					i++
				}
				continue
			}
			if q[i+1] == '*' {
				i += 2
				for i+1 < n && !(q[i] == '*' && q[i+1] == '/') {
					i++
				}
				if i+1 >= n {
					return false
				}
				i += 2
				continue
			}
		}
		break
	}
	if i+3 > n {
		return false
	}
	u, s, e := q[i]|0x20, q[i+1]|0x20, q[i+2]|0x20
	if u != 'u' || s != 's' || e != 'e' {
		return false
	}
	if i+3 == n {
		return true
	}
	next := q[i+3]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r' ||
		next == '"' || next == '/' || next == '-'
}

// validateCompressedBodyBounds inspects the uncompressed-length header inside an LZ4
// or Snappy frame body before decompression so a tiny "zip bomb" frame claiming a
// multi-gigabyte uncompressed size is rejected before allocating memory.
func validateCompressedBodyBounds(algo string, compressed []byte, maxFrameSize int) (int, error) {
	switch algo {
	case "lz4":
		if len(compressed) < 4 {
			return 0, io.ErrUnexpectedEOF
		}
		uncompressedLen := int(int32(binary.BigEndian.Uint32(compressed[:4])))
		if uncompressedLen < 0 || uncompressedLen > maxFrameSize {
			return 0, fmt.Errorf("lz4 uncompressed length %d exceeds maximum allowed frame size of %d bytes", uncompressedLen, maxFrameSize)
		}
		return uncompressedLen, nil
	case "snappy":
		if len(compressed) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		uLen, n := binary.Uvarint(compressed)
		if n <= 0 {
			return 0, fmt.Errorf("invalid snappy uncompressed length header")
		}
		if uLen > uint64(maxFrameSize) {
			return 0, fmt.Errorf("snappy uncompressed length %d exceeds maximum allowed frame size of %d bytes", uLen, maxFrameSize)
		}
		return int(uLen), nil
	default:
		return 0, fmt.Errorf("unsupported compression algorithm %q", algo)
	}
}

// validateRawBodyBounds verifies that the frame opcode is supported and that all length
// prefixes inside the raw frame body fit within len(body), preventing primitive.ReadLongString
// or primitive.ReadBytes from allocating gigabytes on a spoofed length prefix inside a tiny frame.
func validateRawBodyBounds(hdr *frame.Header, body []byte) error {
	if hdr == nil {
		return fmt.Errorf("missing frame header")
	}
	if hdr.Flags.Contains(primitive.HeaderFlagCompressed) {
		return fmt.Errorf("frame still compressed after decompression")
	}

	pos := 0
	if hdr.Flags.Contains(primitive.HeaderFlagCustomPayload) {
		var err error
		pos, err = skipBytesMap(body, pos)
		if err != nil {
			return err
		}
	}

	switch hdr.OpCode {
	case primitive.OpCodeOptions:
		return nil

	case primitive.OpCodeStartup:
		_, err := skipStringMap(body, pos)
		return err

	case primitive.OpCodeRegister:
		_, err := skipStringList(body, pos)
		return err

	case primitive.OpCodeAuthResponse:
		// [bytes] token: bounded by the body, so a huge claimed length is
		// rejected before decoding allocates it.
		_, err := skipBytes(body, pos)
		return err

	case primitive.OpCodeQuery:
		var err error
		pos, err = skipLongString(body, pos)
		if err != nil {
			return err
		}
		return validateQueryOptionsBounds(hdr.Version, body, pos)

	case primitive.OpCodePrepare:
		var err error
		pos, err = skipLongString(body, pos)
		if err != nil {
			return err
		}
		if hdr.Version >= primitive.ProtocolVersion5 {
			if pos+4 > len(body) {
				return io.ErrUnexpectedEOF
			}
			flags := primitive.QueryFlag(binary.BigEndian.Uint32(body[pos : pos+4]))
			pos += 4
			if flags.Contains(primitive.QueryFlagWithKeyspace) {
				_, err = skipString(body, pos)
				return err
			}
		}
		return nil

	case primitive.OpCodeExecute:
		var err error
		pos, err = skipShortBytes(body, pos)
		if err != nil {
			return err
		}
		if hdr.Version >= primitive.ProtocolVersion5 {
			pos, err = skipShortBytes(body, pos)
			if err != nil {
				return err
			}
		}
		return validateQueryOptionsBounds(hdr.Version, body, pos)

	case primitive.OpCodeBatch:
		// type (1 byte) + n (2 bytes)
		if pos+3 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos++ // batch type
		count := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		for i := 0; i < count; i++ {
			if pos >= len(body) {
				return io.ErrUnexpectedEOF
			}
			kind := body[pos]
			pos++
			var err error
			if kind == 0 {
				pos, err = skipLongString(body, pos)
			} else if kind == 1 {
				pos, err = skipShortBytes(body, pos)
			} else {
				return fmt.Errorf("invalid batch child type %d", kind)
			}
			if err != nil {
				return err
			}
			if pos+2 > len(body) {
				return io.ErrUnexpectedEOF
			}
			valCount := int(binary.BigEndian.Uint16(body[pos : pos+2]))
			pos += 2
			for j := 0; j < valCount; j++ {
				pos, err = skipValue(body, pos)
				if err != nil {
					return err
				}
			}
		}
		return validateBatchOptionsBounds(hdr.Version, body, pos)

	default:
		// Reject any opcode we don't explicitly validate and handle (such as AUTH_RESPONSE)
		// before ConvertFromRawFrame can invoke library decoders on untrusted length prefixes.
		return fmt.Errorf("unsupported request opcode: %v", hdr.OpCode)
	}
}

func validateBatchOptionsBounds(version primitive.ProtocolVersion, body []byte, pos int) error {
	// consistency (2 bytes) + flags (1 byte in v3/v4, 4 bytes in v5)
	if pos+2 > len(body) {
		return io.ErrUnexpectedEOF
	}
	pos += 2
	var flags primitive.QueryFlag
	if version >= primitive.ProtocolVersion5 {
		if pos+4 > len(body) {
			return io.ErrUnexpectedEOF
		}
		flags = primitive.QueryFlag(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
	} else {
		if pos+1 > len(body) {
			return io.ErrUnexpectedEOF
		}
		flags = primitive.QueryFlag(body[pos])
		pos++
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		if pos+2 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 2
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		if pos+8 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 8
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		var err error
		pos, err = skipString(body, pos)
		if err != nil {
			return err
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		if pos+4 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 4
	}
	return nil
}

func validateQueryOptionsBounds(version primitive.ProtocolVersion, body []byte, pos int) error {
	// consistency (2 bytes) + flags (1 byte in v3/v4, 4 bytes in v5)
	if pos+2 > len(body) {
		return io.ErrUnexpectedEOF
	}
	pos += 2
	var flags primitive.QueryFlag
	if version >= primitive.ProtocolVersion5 {
		if pos+4 > len(body) {
			return io.ErrUnexpectedEOF
		}
		flags = primitive.QueryFlag(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
	} else {
		if pos+1 > len(body) {
			return io.ErrUnexpectedEOF
		}
		flags = primitive.QueryFlag(body[pos])
		pos++
	}

	if flags.Contains(primitive.QueryFlagValues) {
		if pos+2 > len(body) {
			return io.ErrUnexpectedEOF
		}
		n := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		named := flags.Contains(primitive.QueryFlagValueNames)
		for i := 0; i < n; i++ {
			var err error
			if named {
				pos, err = skipString(body, pos)
				if err != nil {
					return err
				}
			}
			pos, err = skipValue(body, pos)
			if err != nil {
				return err
			}
		}
	}
	if flags.Contains(primitive.QueryFlagPageSize) {
		if pos+4 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 4
	}
	if flags.Contains(primitive.QueryFlagPagingState) {
		var err error
		pos, err = skipBytes(body, pos)
		if err != nil {
			return err
		}
	}
	if flags.Contains(primitive.QueryFlagSerialConsistency) {
		if pos+2 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 2
	}
	if flags.Contains(primitive.QueryFlagDefaultTimestamp) {
		if pos+8 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 8
	}
	if flags.Contains(primitive.QueryFlagWithKeyspace) {
		var err error
		pos, err = skipString(body, pos)
		if err != nil {
			return err
		}
	}
	if flags.Contains(primitive.QueryFlagNowInSeconds) {
		if pos+4 > len(body) {
			return io.ErrUnexpectedEOF
		}
		pos += 4
	}
	return nil
}

func skipLongString(b []byte, pos int) (int, error) {
	if pos+4 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(int32(binary.BigEndian.Uint32(b[pos : pos+4])))
	pos += 4
	if n < 0 || n > len(b)-pos {
		return 0, fmt.Errorf("long string length %d exceeds remaining frame body (%d bytes)", n, len(b)-pos)
	}
	return pos + n, nil
}

func skipString(b []byte, pos int) (int, error) {
	if pos+2 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	if n > len(b)-pos {
		return 0, fmt.Errorf("string length %d exceeds remaining frame body (%d bytes)", n, len(b)-pos)
	}
	return pos + n, nil
}

func skipShortBytes(b []byte, pos int) (int, error) {
	return skipString(b, pos)
}

func skipBytes(b []byte, pos int) (int, error) {
	if pos+4 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(int32(binary.BigEndian.Uint32(b[pos : pos+4])))
	pos += 4
	if n < 0 {
		return pos, nil
	}
	if n > len(b)-pos {
		return 0, fmt.Errorf("bytes length %d exceeds remaining frame body (%d bytes)", n, len(b)-pos)
	}
	return pos + n, nil
}

func skipValue(b []byte, pos int) (int, error) {
	if pos+4 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(int32(binary.BigEndian.Uint32(b[pos : pos+4])))
	pos += 4
	if n < 0 {
		// -1 (null) or -2 (unset)
		return pos, nil
	}
	if n > len(b)-pos {
		return 0, fmt.Errorf("value length %d exceeds remaining frame body (%d bytes)", n, len(b)-pos)
	}
	return pos + n, nil
}

func skipBytesMap(b []byte, pos int) (int, error) {
	if pos+2 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	for i := 0; i < n; i++ {
		var err error
		pos, err = skipString(b, pos)
		if err != nil {
			return 0, err
		}
		pos, err = skipBytes(b, pos)
		if err != nil {
			return 0, err
		}
	}
	return pos, nil
}

func skipStringMap(b []byte, pos int) (int, error) {
	if pos+2 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	for i := 0; i < n; i++ {
		var err error
		pos, err = skipString(b, pos)
		if err != nil {
			return 0, err
		}
		pos, err = skipString(b, pos)
		if err != nil {
			return 0, err
		}
	}
	return pos, nil
}

func skipStringList(b []byte, pos int) (int, error) {
	if pos+2 > len(b) {
		return 0, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	for i := 0; i < n; i++ {
		var err error
		pos, err = skipString(b, pos)
		if err != nil {
			return 0, err
		}
	}
	return pos, nil
}
