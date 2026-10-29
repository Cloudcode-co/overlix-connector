// Package realtime is the Overlix JSON websocket: text frames holding ["event", data].
package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// DefaultURL is the realtime websocket.
const DefaultURL = "wss://api.overlix.net/v1/ws"

// Event is one inbound message.
type Event struct {
	Name string
	Data json.RawMessage
}

// TokenSource returns a valid access token.
type TokenSource func(ctx context.Context) (string, error)

// Client is one authenticated realtime socket.
type Client struct {
	ws        *websocket.Conn
	wmu       sync.Mutex
	SessionID string
	Events    chan Event
	token     TokenSource
	lastToken string
	country   string
	closed    chan struct{}
	once      sync.Once
}

// Connect opens the socket (reclaiming sessionID when given), sends setup-client and waits
// for "authenticate". Events received meanwhile (e.g. world:resumed) are kept for the caller.
func Connect(ctx context.Context, wsURL, sessionID, country string, token TokenSource) (*Client, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, err
	}
	if sessionID != "" {
		q := u.Query()
		q.Set("session_id", sessionID)
		u.RawQuery = q.Encode()
	}
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	ws, _, err := d.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("realtime dial: %w", err)
	}
	ws.SetReadLimit(1 << 20)
	c := &Client{ws: ws, Events: make(chan Event, 256), token: token, country: country, closed: make(chan struct{})}
	ws.SetPingHandler(func(data string) error {
		_ = ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		c.wmu.Lock()
		defer c.wmu.Unlock()
		return ws.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	var early []Event
	deadline := time.Now().Add(30 * time.Second)
	sentSetup := false
	for {
		_ = ws.SetReadDeadline(deadline)
		ev, err := c.read()
		if err != nil {
			ws.Close()
			return nil, fmt.Errorf("realtime handshake: %w", err)
		}
		switch ev.Name {
		case "session_info", "reconnect_success":
			var info struct {
				SessionID string `json:"session_id"`
			}
			_ = json.Unmarshal(ev.Data, &info)
			if info.SessionID != "" {
				c.SessionID = info.SessionID
			}
			if !sentSetup {
				if err := c.setup(ctx); err != nil {
					ws.Close()
					return nil, err
				}
				sentSetup = true
			}
		case "authenticate":
			for _, e := range early {
				c.Events <- e
			}
			go c.readLoop()
			go c.refreshLoop()
			return c, nil
		case "error":
			ws.Close()
			return nil, fmt.Errorf("realtime refused: %s", string(ev.Data))
		default:
			early = append(early, ev)
		}
	}
}

func (c *Client) setup(ctx context.Context) error {
	tok, err := c.token(ctx)
	if err != nil {
		return err
	}
	c.lastToken = tok
	return c.Send("setup-client", map[string]any{
		"token": tok, "country": c.country, "server_version": "v3", "afk": false, "background": false,
	})
}

func (c *Client) read() (Event, error) {
	_, msg, err := c.ws.ReadMessage()
	if err != nil {
		return Event{}, err
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(msg, &arr); err != nil || len(arr) < 1 {
		return Event{}, nil
	}
	var name string
	_ = json.Unmarshal(arr[0], &name)
	ev := Event{Name: name}
	if len(arr) > 1 {
		ev.Data = arr[1]
	}
	return ev, nil
}

func (c *Client) readLoop() {
	defer c.Close()
	defer close(c.Events)
	for {
		_ = c.ws.SetReadDeadline(time.Now().Add(90 * time.Second))
		ev, err := c.read()
		if err != nil {
			log.Printf("[realtime] socket closed: %v", err)
			return
		}
		if ev.Name == "" {
			continue
		}
		select {
		case c.Events <- ev:
		case <-c.closed:
			return
		}
	}
}

// refreshLoop re-sends setup-client on the same socket whenever the access token changes.
func (c *Client) refreshLoop() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			tok, err := c.token(ctx)
			cancel()
			if err != nil {
				log.Printf("[realtime] token refresh failed: %v", err)
				continue
			}
			if tok != c.lastToken {
				c.lastToken = tok
				_ = c.Send("setup-client", map[string]any{
					"token": tok, "country": c.country, "server_version": "v3", "afk": false, "background": false,
				})
			}
		}
	}
}

// Send writes ["event", data].
func (c *Client) Send(event string, data any) error {
	raw, err := json.Marshal([]any{event, data})
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	select {
	case <-c.closed:
		return errors.New("realtime closed")
	default:
	}
	_ = c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.ws.WriteMessage(websocket.TextMessage, raw)
}

func (c *Client) Close() {
	c.once.Do(func() {
		close(c.closed)
		_ = c.ws.Close()
	})
}

// Done is closed when the socket is gone.
func (c *Client) Done() <-chan struct{} { return c.closed }
