package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLogicalMessages emits pg_logical_emit_message both ways around a
// transaction. The non-transactional message must reach the listener on its
// own, with its prefix, content and LSN, before the next transaction; the
// transactional one inside its transaction. Acknowledging a non-transactional
// message alone must move the slot, which is what makes it usable as a
// heartbeat on a database with no changes of its own.
func TestLogicalMessages(t *testing.T) {
	ctx := context.Background()
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_logical_messages"

	forEachProtoVersion(t, cdcCfg, func(t *testing.T, cdcCfg config.Config) {
		postgresConn, err := newPostgresConn()
		require.NoError(t, err)
		require.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg))

		messageCh := make(chan any, 10)
		connector, err := cdc.NewConnector(ctx, cdcCfg, func(ctx *replication.ListenerContext) {
			switch ctx.Message.(type) {
			case *format.LogicalDecodingMessage, *format.Insert:
				messageCh <- ctx.Message
			}
			_ = ctx.Ack()
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			connector.Close()
			assert.NoError(t, RestoreDB(ctx))
			assert.NoError(t, postgresConn.Close(ctx))
		})

		go connector.Start(ctx)

		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		require.NoError(t, connector.WaitUntilReady(waitCtx))
		cancel()

		heartbeatLSN := emitLogicalMessage(t, postgresConn, "SELECT pg_logical_emit_message(false, 'heartbeat', '1761400000.123')")
		require.NoError(t, pgExec(ctx, postgresConn,
			"BEGIN; SELECT pg_logical_emit_message(true, 'command', '{}'); INSERT INTO books(id, name) VALUES (1, 'a'); COMMIT;"))

		heartbeat := receiveAs[*format.LogicalDecodingMessage](t, messageCh)
		assert.False(t, heartbeat.Transactional())
		assert.Equal(t, "heartbeat", heartbeat.Prefix)
		assert.Equal(t, []byte("1761400000.123"), heartbeat.Content)
		assert.Equal(t, heartbeatLSN, heartbeat.LSN)

		command := receiveAs[*format.LogicalDecodingMessage](t, messageCh)
		assert.True(t, command.Transactional())
		assert.Equal(t, "command", command.Prefix)
		assert.Equal(t, []byte("{}"), command.Content)

		insert := receiveAs[*format.Insert](t, messageCh)
		assert.Equal(t, "a", insert.Decoded["name"])

		// Nothing but a message after the transaction: its ack alone has to
		// carry the slot past it.
		lastLSN := emitLogicalMessage(t, postgresConn, "SELECT pg_logical_emit_message(false, 'heartbeat', 'last')")
		last := receiveAs[*format.LogicalDecodingMessage](t, messageCh)
		assert.Equal(t, []byte("last"), last.Content)

		assert.Eventually(t, func() bool {
			_, confirmed, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
			if err != nil {
				return false
			}
			lsn, err := pq.ParseLSN(confirmed)
			return err == nil && lsn >= lastLSN
		}, 5*time.Second, 100*time.Millisecond, "confirmed_flush_lsn should reach the acknowledged message at %s", lastLSN)
	})
}

// emitLogicalMessage runs a pg_logical_emit_message statement and returns the
// LSN it reports for the message.
func emitLogicalMessage(t *testing.T, conn pq.Connection, sql string) pq.LSN {
	t.Helper()
	rr := conn.Exec(context.Background(), sql)
	results, err := rr.ReadAll()
	require.NoError(t, err)
	require.NoError(t, rr.Close())
	require.NotEmpty(t, results)
	require.NotEmpty(t, results[0].Rows)

	lsn, err := pq.ParseLSN(string(results[0].Rows[0][0]))
	require.NoError(t, err)
	return lsn
}

func receiveAs[T any](t *testing.T, ch <-chan any) T {
	t.Helper()
	select {
	case m := <-ch:
		v, ok := m.(T)
		require.Truef(t, ok, "expected %T, got %T", *new(T), m)
		return v
	case <-time.After(5 * time.Second):
		require.FailNow(t, "no message within 5s")
	}
	var zero T
	return zero
}
