package format

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogicalDecodingMessage(t *testing.T) {
	now := time.Now().UTC()

	t.Run("transactional message carries lsn, prefix, and content", func(t *testing.T) {
		data := []byte{
			'M',
			1,                        // transactional
			0, 0, 0, 0, 0, 0, 0, 100, // LSN
			'd', 's', '-', 'c', 'o', 'm', 'm', 'a', 'n', 'd', 0,
			0, 0, 0, 2,
			'{', '}',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.NoError(t, err)
		assert.Equal(t, now, msg.MessageTime)
		assert.Equal(t, uint8(1), msg.Flags)
		assert.Equal(t, pq.LSN(100), msg.LSN)
		assert.Equal(t, "ds-command", msg.Prefix)
		assert.Equal(t, []byte("{}"), msg.Content)
		assert.Equal(t, uint32(0), msg.XID)
	})

	t.Run("non-transactional message carries its lsn too", func(t *testing.T) {
		data := []byte{
			'M',
			0,                        // not transactional
			0, 0, 0, 0, 0, 0, 0, 200, // LSN
			'n', 'o', 't', 'e', 0,
			0, 0, 0, 1,
			'x',
		}

		msg, err := NewLogicalDecodingMessage(data, false, now)

		require.NoError(t, err)
		assert.False(t, msg.Transactional())
		assert.Equal(t, pq.LSN(200), msg.LSN)
		assert.Equal(t, "note", msg.Prefix)
		assert.Equal(t, []byte("x"), msg.Content)
	})

	// Captured from PostgreSQL 18 with pg_logical_slot_peek_binary_changes(...,
	// 'proto_version', '2', 'messages', 'true').
	t.Run("decodes bytes sent by postgres", func(t *testing.T) {
		cases := []struct {
			name          string
			hex           string
			prefix        string
			content       string
			lsn           pq.LSN
			transactional bool
		}{
			{
				name:    "pg_logical_emit_message(false, 'debezium-heartbeat', '1761400000.123')",
				hex:     "4d0000000000017963d8646562657a69756d2d686561727462656174000000000e313736313430303030302e313233",
				lsn:     0x17963D8,
				prefix:  "debezium-heartbeat",
				content: "1761400000.123",
			},
			{
				name:          "pg_logical_emit_message(true, 'ds-command', '{}')",
				hex:           "4d01000000000179641864732d636f6d6d616e6400000000027b7d",
				lsn:           0x1796418,
				prefix:        "ds-command",
				content:       "{}",
				transactional: true,
			},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				data, err := hex.DecodeString(c.hex)
				require.NoError(t, err)

				msg, err := NewLogicalDecodingMessage(data, false, now)

				require.NoError(t, err)
				assert.Equal(t, c.transactional, msg.Transactional())
				assert.Equal(t, c.lsn, msg.LSN)
				assert.Equal(t, c.prefix, msg.Prefix)
				assert.Equal(t, []byte(c.content), msg.Content)
			})
		}
	})

	t.Run("streamed message is prefixed with the transaction id", func(t *testing.T) {
		data := []byte{
			'M',
			0, 0, 0, 7, // XID
			1,
			0, 0, 0, 0, 0, 0, 1, 0, // LSN 256
			'p', 0,
			0, 0, 0, 0, // empty content
		}

		msg, err := NewLogicalDecodingMessage(data, true, now)

		require.NoError(t, err)
		assert.Equal(t, uint32(7), msg.XID)
		assert.Equal(t, pq.LSN(256), msg.LSN)
		assert.Equal(t, "p", msg.Prefix)
		assert.Empty(t, msg.Content)
	})

	t.Run("content does not alias the input buffer", func(t *testing.T) {
		data := []byte{'M', 0, 0, 0, 0, 0, 0, 0, 0, 1, 'p', 0, 0, 0, 0, 2, 'o', 'k'}

		msg, err := NewLogicalDecodingMessage(data, false, now)
		require.NoError(t, err)
		copy(data[len(data)-2:], "xx")

		assert.Equal(t, []byte("ok"), msg.Content)
	})

	t.Run("returns an error when the prefix is not terminated", func(t *testing.T) {
		data := []byte{'M', 0, 0, 0, 0, 0, 0, 0, 0, 1, 'p'}

		_, err := NewLogicalDecodingMessage(data, false, now)

		require.Error(t, err)
	})

	t.Run("returns an error when the lsn is cut short", func(t *testing.T) {
		data := []byte{'M', 0, 0, 0, 0}

		_, err := NewLogicalDecodingMessage(data, false, now)

		require.Error(t, err)
	})
}
