package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logicalHarness drives dispatchMessage outside a streamed transaction and
// collects what reaches the consumer channel.
type logicalHarness struct {
	s         *stream
	buf       *messageBuffer
	streamBuf *streamTxBuffer
	out       chan *Message
}

func newLogicalHarness() *logicalHarness {
	out := make(chan *Message, 100)
	return &logicalHarness{
		s:         &stream{},
		buf:       &messageBuffer{outCh: out},
		streamBuf: &streamTxBuffer{},
		out:       out,
	}
}

func (h *logicalHarness) dispatch(msg any, walStart pq.LSN) {
	h.s.dispatchMessage(msg, XLogData{WALStart: walStart}, h.buf, h.streamBuf)
}

func (h *logicalHarness) delivered() []*Message {
	var msgs []*Message
	for {
		select {
		case m := <-h.out:
			msgs = append(msgs, m)
		default:
			return msgs
		}
	}
}

func TestNonTransactionalLogicalMessageIsDeliveredAtOnce(t *testing.T) {
	h := newLogicalHarness()
	h.dispatch(&format.Begin{Xid: 700, FinalLSN: 90}, 80)
	h.dispatch(&format.Insert{XID: 700, TableName: "books"}, 85)
	h.dispatch(&format.Commit{TransactionEndLSN: 95}, 90)

	heartbeat := &format.LogicalDecodingMessage{LSN: 100, Prefix: "heartbeat"}
	h.dispatch(heartbeat, 100)
	// The next transaction's BEGIN resets the commit look-ahead; the message
	// must already be out by then.
	h.dispatch(&format.Begin{Xid: 701, FinalLSN: 120}, 110)

	got := h.delivered()
	require.Len(t, got, 2)
	assert.IsType(t, &format.Insert{}, got[0].message)
	assert.Same(t, heartbeat, got[1].message)
	assert.Equal(t, int64(100), got[1].walStart, "acknowledged at its own LSN")
	assert.Zero(t, got[1].xid, "belongs to no transaction, not to the previous one")
	assert.Zero(t, got[1].commitLSN)
}

func TestTransactionalLogicalMessageStaysInItsTransaction(t *testing.T) {
	h := newLogicalHarness()
	h.dispatch(&format.Begin{Xid: 700, FinalLSN: 90}, 80)
	command := &format.LogicalDecodingMessage{Flags: 1, LSN: 82, Prefix: "command"}
	h.dispatch(command, 82)
	h.dispatch(&format.Insert{XID: 700, TableName: "books"}, 85)
	h.dispatch(&format.Commit{TransactionEndLSN: 95}, 90)

	got := h.delivered()
	require.Len(t, got, 2)
	assert.Same(t, command, got[0].message)
	assert.Equal(t, int64(82), got[0].walStart)
	assert.Equal(t, uint32(700), got[0].xid)
	assert.Equal(t, pq.LSN(90), got[0].commitLSN)
	assert.IsType(t, &format.Insert{}, got[1].message)
	assert.Equal(t, int64(95), got[1].walStart, "the last change carries the transaction end")
}

func TestGateSkipsNonTransactionalLogicalMessage(t *testing.T) {
	gs := newGatedStream(t, guardCfg(config.VisibilityFailClosed), [][]string{{"f", "100:201:"}})
	var seen []any
	gs.s.listenerFunc = func(ctx *ListenerContext) {
		seen = append(seen, ctx.Message)
		_ = ctx.Ack()
	}
	heartbeat := &format.LogicalDecodingMessage{LSN: 1, Prefix: "heartbeat"}

	// Before any transaction: xid and commitLSN are zero, which the gate would
	// refuse as a message decoded without its BEGIN.
	require.NoError(t, gs.run(&Message{message: heartbeat, walStart: 1}, insertMsg(200, 2)))

	require.Len(t, seen, 2)
	assert.Same(t, heartbeat, seen[0])
	assert.Equal(t, int32(1), gs.q.calls.Load(), "only the insert's transaction is gated")
	assert.Equal(t, pq.LSN(2), gs.s.LoadConfirmedXLogPos())
}
