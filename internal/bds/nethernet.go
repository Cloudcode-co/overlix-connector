package bds

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/discovery"
)

// NethernetBDS finds a BDS with transport=nethernet over LAN discovery (UDP 7551) and opens
// one NetherNet (WebRTC) connection to it per Overlix guest, like the Overlix app does
// with a client-hosted world. Messages are whole: go-nethernet splits and joins the
// 10 000-byte segments of its own link; the relay's segments are joined here.
type NethernetBDS struct {
	disc   *discovery.Listener
	id     uint64
	back   func(peer uint64, message []byte)
	mu     sync.Mutex
	guests map[uint64]*nnGuest
}

type nnGuest struct {
	conn    *nethernet.Conn
	ready   bool
	pending [][]byte
	asm     []byte
	expect  int
	cancel  context.CancelFunc
}

// FindNethernet broadcasts discovery requests to broadcast (default 255.255.255.255:7551)
// until a server answers or wait passes.
func FindNethernet(broadcast string, wait time.Duration) (*NethernetBDS, *Card, error) {
	conf := discovery.ListenConfig{Log: slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	if broadcast != "" {
		addr, err := net.ResolveUDPAddr("udp", broadcast)
		if err != nil {
			return nil, nil, err
		}
		conf.BroadcastAddress = addr
	}
	disc, err := conf.Listen(discoveryBind())
	if err != nil {
		return nil, nil, err
	}
	n := &NethernetBDS{disc: disc, guests: map[uint64]*nnGuest{}}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if card := n.card(); card != nil {
			n.id = card.GUID
			return n, card, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = disc.Close()
	return nil, nil, errors.New("no NetherNet server answered LAN discovery")
}

func (n *NethernetBDS) card() *Card {
	for id, raw := range n.disc.Responses() {
		if n.id != 0 && id != n.id {
			continue
		}
		var d discovery.ServerData
		if err := d.UnmarshalBinary(raw); err != nil {
			continue
		}
		if os.Getenv("OVERLIX_DEBUG") != "" {
			log.Printf("[nethernet] server data: %+v", d)
		}
		return &Card{
			ServerName: d.ServerName, LevelName: d.LevelName, Protocol: d.Protocol, VersionName: d.Version,
			Players: d.PlayerCount, MaxPlayers: d.MaxPlayerCount, GameModeID: d.GameType, GUID: id,
			Nonce: d.Nonce, Hardcore: d.Hardcore,
		}
	}
	return nil
}

// Card is the server's current world card (nil when it stopped answering).
func (n *NethernetBDS) Card() *Card { return n.card() }

// SetBack sets where BDS messages for a guest go.
func (n *NethernetBDS) SetBack(back func(peer uint64, message []byte)) { n.back = back }

// Connect opens (or reopens) a guest's link to BDS: the guest's Minecraft sent CONNECTREQUEST.
func (n *NethernetBDS) Connect(peer uint64) {
	n.Drop(peer)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	g := &nnGuest{cancel: cancel, expect: -1}
	n.mu.Lock()
	n.guests[peer] = g
	n.mu.Unlock()
	go func() {
		defer cancel()
		conn, err := nethernet.Dialer{AllowIdentitylessServer: true, Log: slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelDebug}))}.
			DialContext(ctx, strconv.FormatUint(n.id, 10), n.disc)
		if err != nil {
			log.Printf("[nethernet] guest %d: dial BDS: %v (if Overlix or Minecraft runs on this machine it shares UDP 7551 and takes the BDS replies: close it)", peer, err)
			n.Drop(peer)
			return
		}
		n.mu.Lock()
		if n.guests[peer] != g {
			n.mu.Unlock()
			_ = conn.Close()
			return
		}
		g.conn = conn
		g.ready = true
		pending := g.pending
		g.pending = nil
		n.mu.Unlock()
		for _, m := range pending {
			_, _ = conn.Send(m, nethernet.MessageReliabilityReliable)
		}
		log.Printf("[nethernet] guest %d linked to BDS", peer)
		go n.pump(peer, g, nethernet.MessageReliabilityUnreliable)
		n.pump(peer, g, nethernet.MessageReliabilityReliable)
	}()
}

func (n *NethernetBDS) pump(peer uint64, g *nnGuest, r nethernet.MessageReliability) {
	for {
		msg, err := g.conn.Receive(r)
		if err != nil {
			if r == nethernet.MessageReliabilityReliable {
				n.dropIf(peer, g)
			}
			return
		}
		if n.back != nil {
			n.back(peer, msg)
		}
	}
}

// FromGuest takes one relay segment ([countdown] + chunk) of a guest's stream.
func (n *NethernetBDS) FromGuest(peer uint64, segment []byte) {
	if len(segment) < 2 {
		return
	}
	n.mu.Lock()
	g := n.guests[peer]
	if g == nil {
		n.mu.Unlock()
		return
	}
	countdown := int(segment[0])
	if g.expect >= 0 && g.asm != nil && countdown != g.expect {
		g.asm = nil // broken run
	}
	g.asm = append(g.asm, segment[1:]...)
	if countdown > 0 {
		g.expect = countdown - 1
		n.mu.Unlock()
		return
	}
	msg := g.asm
	g.asm = nil
	g.expect = -1
	if !g.ready {
		if len(g.pending) < 256 {
			g.pending = append(g.pending, msg)
		}
		n.mu.Unlock()
		return
	}
	conn := g.conn
	n.mu.Unlock()
	_, _ = conn.Send(msg, nethernet.MessageReliabilityReliable)
}

func (n *NethernetBDS) dropIf(peer uint64, g *nnGuest) {
	n.mu.Lock()
	if n.guests[peer] == g {
		delete(n.guests, peer)
	}
	n.mu.Unlock()
	g.close()
}

// Drop closes a guest's link.
func (n *NethernetBDS) Drop(peer uint64) {
	n.mu.Lock()
	g := n.guests[peer]
	delete(n.guests, peer)
	n.mu.Unlock()
	if g != nil {
		g.close()
	}
}

func (g *nnGuest) close() {
	if g.cancel != nil {
		g.cancel()
	}
	if g.conn != nil {
		_ = g.conn.Close()
	}
}

// Close drops every guest and stops discovery.
func (n *NethernetBDS) Close() {
	n.mu.Lock()
	gs := n.guests
	n.guests = map[uint64]*nnGuest{}
	n.mu.Unlock()
	for _, g := range gs {
		g.close()
	}
	_ = n.disc.Close()
}

type discardWriter struct{}

// Write drops go-nethernet's logs unless OVERLIX_DEBUG is set.
func (discardWriter) Write(p []byte) (int, error) {
	if os.Getenv("OVERLIX_DEBUG") != "" {
		return os.Stderr.Write(p)
	}
	return len(p), nil
}

// discoveryBind is the local discovery socket (OVERLIX_DISCOVERY_BIND, default any port).
func discoveryBind() string {
	if v := os.Getenv("OVERLIX_DISCOVERY_BIND"); v != "" {
		return v
	}
	return ":0"
}
