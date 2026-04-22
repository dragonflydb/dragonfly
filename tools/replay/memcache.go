// Package main: minimal raw-TCP memcache ASCII protocol client used by the
// traffic replayer. Exists because we need to reproduce *exactly* the wire
// commands that were recorded (including CAS tokens, GAT one-shot semantics,
// multi-key requests, flags/expire, etc.) — high-level libraries like
// gomemcache hide those details.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

// mcClient is a synchronous text-protocol memcache client. Each command is
// written in full and its reply is consumed before returning, so replay is
// sequential per connection — matching the memcache ASCII protocol, which has
// no request multiplexing.
type mcClient struct {
	addr string
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func newMCClient(addr string) *mcClient {
	c := &mcClient{addr: addr}
	c.dial()
	return c
}

func (c *mcClient) dial() {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		log.Fatalf("memcache dial %s: %v", c.addr, err)
	}
	c.conn = conn
	c.r = bufio.NewReader(conn)
	c.w = bufio.NewWriter(conn)
}

// writeAll writes the prepared command buffer (including any value block and
// terminating \r\n), flushes and reads the server reply.
func (c *mcClient) writeAll(cmd []byte) {
	if _, err := c.w.Write(cmd); err != nil {
		log.Printf("memcache write to %s failed: %v", c.addr, err)
		return
	}
	if err := c.w.Flush(); err != nil {
		log.Printf("memcache flush to %s failed: %v", c.addr, err)
		return
	}
	c.drainReply()
}

// drainReply consumes one server reply. For retrieval commands this means
// reading VALUE lines + data blocks until END; for everything else a single
// line. Non-fatal: errors are logged and the reader is left positioned
// wherever possible so replay can continue.
func (c *mcClient) drainReply() {
	for {
		line, err := c.r.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("memcache read from %s failed: %v", c.addr, err)
			}
			return
		}
		switch {
		case bytes.HasPrefix(line, []byte("VALUE ")):
			// VALUE <key> <flags> <bytes> [<cas>]\r\n  -- followed by <bytes> + "\r\n"
			fields := bytes.Fields(line)
			if len(fields) < 4 {
				return
			}
			n, err := strconv.Atoi(string(fields[3]))
			if err != nil {
				return
			}
			// Read exactly n bytes + trailing \r\n.
			if _, err := io.CopyN(io.Discard, c.r, int64(n)+2); err != nil {
				return
			}
			// Loop — next line is either another VALUE or END.
		case bytes.HasPrefix(line, []byte("STAT ")):
			// STATS reply is a sequence of STAT lines terminated by END.
			// Keep reading.
		default:
			// All other terminal responses are single-line: STORED, NOT_STORED,
			// EXISTS, NOT_FOUND, DELETED, END, OK, VERSION..., <decimal>,
			// ERROR, CLIENT_ERROR..., SERVER_ERROR...
			return
		}
	}
}

// send formats `<header>\r\n[value\r\n]` and issues it.
// value may be nil for non-store commands.
func (c *mcClient) send(header string, value []byte) {
	// Preallocate: header + \r\n + value + \r\n
	buf := make([]byte, 0, len(header)+4+len(value))
	buf = append(buf, header...)
	buf = append(buf, '\r', '\n')
	if value != nil {
		buf = append(buf, value...)
		buf = append(buf, '\r', '\n')
	}
	c.writeAll(buf)
}

// Store replays SET/ADD/REPLACE/APPEND/PREPEND.
func (c *mcClient) Store(cmd, key string, flags uint32, exp int64, value []byte) {
	hdr := fmt.Sprintf("%s %s %d %d %d", cmd, key, flags, exp, len(value))
	c.send(hdr, value)
}

// Cas replays `cas <key> <flags> <exp> <bytes> <cas_unique>\r\n<value>\r\n`.
// The server will reply STORED or EXISTS — the same response the original
// client saw, preserving the CAS-miss behaviour.
func (c *mcClient) Cas(key string, flags uint32, exp int64, casUnique uint64, value []byte) {
	hdr := fmt.Sprintf("cas %s %d %d %d %d", key, flags, exp, len(value), casUnique)
	c.send(hdr, value)
}

// Retrieve issues `<cmd> <key>...\r\n` where cmd is get|gets. For gat/gats use
// RetrieveGat — those commands need an <exp> token between the command and the
// key list so they get their own helper.
func (c *mcClient) Retrieve(cmd string, keys []string) {
	var b bytes.Buffer
	b.WriteString(cmd)
	for _, k := range keys {
		b.WriteByte(' ')
		b.WriteString(k)
	}
	c.send(b.String(), nil)
}

// RetrieveGat issues `gat|gats <exp> <key>...\r\n`.
func (c *mcClient) RetrieveGat(cmd string, exp int64, keys []string) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %d", cmd, exp)
	for _, k := range keys {
		b.WriteByte(' ')
		b.WriteString(k)
	}
	c.send(b.String(), nil)
}

// Delete replays `delete <key> [extras...]\r\n`. Memcache's deprecated delayed
// delete (`delete <key> <time>`) parses into backed_args as extra tokens —
// forward them verbatim so behaviour matches the original request.
func (c *mcClient) Delete(key string, extras []string) {
	var b bytes.Buffer
	b.WriteString("delete ")
	b.WriteString(key)
	for _, e := range extras {
		b.WriteByte(' ')
		b.WriteString(e)
	}
	c.send(b.String(), nil)
}

// IncrDecr replays `incr|decr <key> <delta>\r\n`.
func (c *mcClient) IncrDecr(cmd, key string, delta uint64) {
	c.send(fmt.Sprintf("%s %s %d", cmd, key, delta), nil)
}

// FlushAll replays `flush_all\r\n`.
func (c *mcClient) FlushAll() {
	c.send("flush_all", nil)
}
