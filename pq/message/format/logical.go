package format

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// LogicalDecodingMessage is a pgoutput logical decoding message (type 'M'),
// produced by pg_logical_emit_message. Wire layout, from
// logicalrep_write_message in src/backend/replication/logical/proto.c:
//
//	Byte1('M')   message type
//	Int32        XID, only inside a streamed transaction (proto version 2)
//	Int8         flags (bit 0 = transactional)
//	Int64        LSN of the message, always present
//	String       prefix, null terminated
//	Int32        content length
//	Bytes        content
//
// There is no commit timestamp on the wire. MessageTime is the server time of
// the WAL record.
type LogicalDecodingMessage struct {
	MessageTime time.Time
	Prefix      string
	Content     []byte
	// LSN is the position of the message's own WAL record, for transactional
	// and non-transactional messages alike.
	LSN   pq.LSN
	XID   uint32
	Flags uint8
}

// Transactional reports whether the message was emitted with
// pg_logical_emit_message(true, ...): it is then part of its transaction,
// arrives between BEGIN and COMMIT, and is dropped with it on rollback. A
// non-transactional message is sent as soon as it is decoded, outside any
// transaction.
func (m *LogicalDecodingMessage) Transactional() bool {
	return m.Flags&1 != 0
}

func NewLogicalDecodingMessage(data []byte, streamedTransaction bool, serverTime time.Time) (*LogicalDecodingMessage, error) {
	msg := &LogicalDecodingMessage{MessageTime: serverTime}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *LogicalDecodingMessage) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < skipByte+4 {
			return errors.Newf("streamed transaction logical decoding message is missing its XID, got %d bytes", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	// flags, the LSN, and at least the prefix's null terminator
	if len(data) < skipByte+10 {
		return errors.Newf("logical decoding message length must be at least %d bytes, but got %d", skipByte+10, len(data))
	}

	m.Flags = data[skipByte]
	skipByte++

	m.LSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8

	relIdx := bytes.IndexByte(data[skipByte:], 0)
	if relIdx < 0 {
		return errors.Newf("logical decoding message prefix is not null terminated, remaining %d", len(data)-skipByte)
	}
	nullIdx := skipByte + relIdx

	m.Prefix = string(data[skipByte:nullIdx])
	skipByte = nullIdx + 1

	if len(data) < skipByte+4 {
		return errors.Newf("logical decoding message content length out of bounds, remaining %d", len(data)-skipByte)
	}

	contentLength := int(binary.BigEndian.Uint32(data[skipByte:]))
	skipByte += 4

	if len(data) < skipByte+contentLength {
		return errors.Newf("logical decoding message content length out of bounds, content %d, remaining %d", contentLength, len(data)-skipByte)
	}

	// A copy: data aliases the connection's read buffer, which the next
	// receive overwrites while the listener may still hold the message.
	m.Content = bytes.Clone(data[skipByte : skipByte+contentLength])

	return nil
}
