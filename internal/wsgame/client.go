package wsgame

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Event is one thing the game server told the host.
type Event struct {
	ID         uint64
	Peer       uint64 // PeerJoined/PeerLeft/relays: the guest
	User       int64  // PeerJoined
	ServerType uint8  // relays
	Data       []byte // relays
	Reason     string // WorldClosed / Disconnect / Error
}

// Conn is the host's /ws/game link for one world.
type Conn struct {
	ws       *websocket.Conn
	wmu      sync.Mutex
	HostPeer uint64
	WorldID  string
	closed   chan struct{}
	once     sync.Once
}

// Dial connects to wss://endpoint/ws/game with a one-time ticket from world:success or
// host_reconnect_ticket and completes the handshake.
func Dial(ctx context.Context, endpoint, ticket string) (*Conn, error) {
	scheme := "wss"
	host := endpoint
	if strings.HasPrefix(host, "localhost") || strings.HasPrefix(host, "127.") || strings.HasPrefix(host, "10.") || strings.HasPrefix(host, "192.168.") {
		scheme = "ws"
	}
	u := url.URL{Scheme: scheme, Host: host, Path: "/ws/game"}
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	ws, _, err := d.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", u.String(), err)
	}
	c := &Conn{ws: ws, closed: make(chan struct{})}
	_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))

	var hello writer
	hello.u16(ProtocolVersion)
	hello.blob([]byte{0}) // compressions: none
	hello.str(ticket)
	if err := c.send(IDClientHello, hello.Bytes()); err != nil {
		ws.Close()
		return nil, err
	}
	if _, _, err := c.expect(IDServerHello); err != nil {
		ws.Close()
		return nil, err
	}
	if err := c.send(IDReady, nil); err != nil {
		ws.Close()
		return nil, err
	}
	_, payload, err := c.expect(IDJoinWorldAck)
	if err != nil {
		ws.Close()
		return nil, err
	}
	r := newReader(payload)
	ok, _ := r.u8()
	c.HostPeer, _ = r.u64()
	c.WorldID, _ = r.str()
	code, _ := r.u16()
	msg, _ := r.str()
	if ok == 0 {
		ws.Close()
		return nil, fmt.Errorf("join world refused: %d %s", code, msg)
	}
	ws.SetPingHandler(func(data string) error {
		_ = ws.SetReadDeadline(time.Now().Add(45 * time.Second))
		c.wmu.Lock()
		defer c.wmu.Unlock()
		return ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	return c, nil
}

func (c *Conn) expect(want uint64) (uint64, []byte, error) {
	for {
		_, frame, err := c.ws.ReadMessage()
		if err != nil {
			return 0, nil, err
		}
		id, payload, err := decodeFrame(frame)
		if err != nil {
			return 0, nil, err
		}
		switch id {
		case want:
			return id, payload, nil
		case IDDisconnect, IDError:
			return 0, nil, fmt.Errorf("server refused handshake: %s", reasonOf(id, payload))
		}
	}
}

// Run reads until the link ends, answering pings and handing every other packet to fn.
func (c *Conn) Run(fn func(Event)) error {
	defer c.Close()
	go c.keepalive()
	for {
		_ = c.ws.SetReadDeadline(time.Now().Add(45 * time.Second))
		_, frame, err := c.ws.ReadMessage()
		if err != nil {
			return err
		}
		id, payload, err := decodeFrame(frame)
		if err != nil {
			return err
		}
		r := newReader(payload)
		switch id {
		case IDPing:
			nonce, _ := r.u64()
			var w writer
			w.u64(nonce)
			_ = c.send(IDPong, w.Bytes())
		case IDPeerJoined:
			peer, _ := r.u64()
			user, _ := r.u64()
			fn(Event{ID: id, Peer: peer, User: int64(user)})
		case IDPeerLeft:
			peer, _ := r.u64()
			fn(Event{ID: id, Peer: peer})
		case IDPeerDataRelay, IDPeerSignalRelay:
			peer, _ := r.u64()
			st, _ := r.u8()
			data, err := r.blob()
			if err == nil {
				fn(Event{ID: id, Peer: peer, ServerType: st, Data: data})
			}
		case IDWorldClosed:
			_, _ = r.u16()
			reason, _ := r.str()
			fn(Event{ID: id, Reason: reason})
			return errors.New("world closed: " + reason)
		case IDDisconnect:
			return errors.New("disconnected: " + reasonOf(id, payload))
		case IDError:
			fn(Event{ID: id, Reason: reasonOf(id, payload)})
		}
	}
}

func (c *Conn) keepalive() {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			var w writer
			w.u64(uint64(time.Now().UnixNano()))
			if c.send(IDPing, w.Bytes()) != nil {
				return
			}
		}
	}
}

func reasonOf(id uint64, payload []byte) string {
	r := newReader(payload)
	code, _ := r.u16()
	msg, _ := r.str()
	return fmt.Sprintf("packet 0x%02x code=%d %s", id, code, msg)
}

// SendData sends one segment/datagram to guest peer.
func (c *Conn) SendData(peer uint64, serverType uint8, data []byte) error {
	var w writer
	w.u64(peer)
	w.u8(serverType)
	w.blob(data)
	return c.send(IDPeerDataSend, w.Bytes())
}

// SendSignal sends a signaling blob to guest peer.
func (c *Conn) SendSignal(peer uint64, serverType uint8, data []byte) error {
	var w writer
	w.u64(peer)
	w.u8(serverType)
	w.blob(data)
	return c.send(IDPeerSignalSend, w.Bytes())
}

// Leave tells the server the host is done with the world.
func (c *Conn) Leave() { _ = c.send(IDLeaveWorld, nil) }

func (c *Conn) send(id uint64, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.ws.WriteMessage(websocket.BinaryMessage, encodeFrame(id, payload))
}

func (c *Conn) Close() {
	c.once.Do(func() {
		close(c.closed)
		_ = c.ws.Close()
	})
}
