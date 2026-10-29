// Package host publishes a local BDS on Overlix and relays every guest to it, the way the
// Overlix app hosts a LAN world: realtime world:open, then the /ws/game relay as host.
package host

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Cloudcode-co/overlix-connector/internal/bds"
	"github.com/Cloudcode-co/overlix-connector/internal/realtime"
	"github.com/Cloudcode-co/overlix-connector/internal/wsgame"
)

// Config of one connector.
type Config struct {
	RealtimeURL string
	Country     string
	Token       realtime.TokenSource
	// Transport: "auto" (NetherNet discovery first, then RakNet ping), "nethernet" or "raknet".
	Transport string
	// RaknetAddr is BDS's RakNet port (server-port), e.g. 127.0.0.1:19132.
	RaknetAddr string
	// DiscoveryAddr is where NetherNet discovery requests go (default broadcast :7551).
	DiscoveryAddr string
	Password      string
	Description   string
	FriendsOnly   bool
	// ApproveAll admits guests the world's whitelist would send to the host for approval.
	ApproveAll bool
	// Guard asks for the worldGuard™ anti-spam bot in the world.
	Guard bool
}

// server is the BDS side, whichever transport it speaks.
type server interface {
	card() *bds.Card
	nethernet() bool
	fromGuest(peer uint64, data []byte)
	signal(peer uint64, data []byte)
	drop(peer uint64)
	close()
}

// Run keeps the world published until ctx ends, starting over after any failure.
func Run(ctx context.Context, cfg Config) error {
	backoff := 2 * time.Second
	for {
		err := runOnce(ctx, cfg)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, ErrNoRelayAccess) {
			return err
		}
		log.Printf("[host] stopped: %v; retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func runOnce(ctx context.Context, cfg Config) error {
	srv, err := findServer(cfg)
	if err != nil {
		return err
	}
	defer srv.close()
	card := srv.card()
	log.Printf("[host] BDS found: %q / %q %s (protocol %d) nethernet=%v players %d/%d",
		card.ServerName, card.LevelName, card.VersionName, card.Protocol, srv.nethernet(), card.Players, card.MaxPlayers)

	rt, err := realtime.Connect(ctx, cfg.RealtimeURL, "", cfg.Country, cfg.Token)
	if err != nil {
		return err
	}
	defer rt.Close()

	if err := rt.Send("game:minecraft:world:open", openPayload(cfg, card, srv.nethernet())); err != nil {
		return err
	}
	success, err := waitFor(ctx, rt, "game:minecraft:world:success", 20*time.Second)
	if err != nil {
		return err
	}
	var opened struct {
		WorldID  string `json:"world_id"`
		Ticket   string `json:"game_ticket"`
		Endpoint string `json:"game_endpoint"`
	}
	_ = json.Unmarshal(success.Data, &opened)
	if opened.WorldID == "" || opened.Ticket == "" {
		return errors.New("world:success without world id or game ticket")
	}
	log.Printf("[host] world published: %s (node %s)", opened.WorldID, opened.Endpoint)
	defer func() { _ = rt.Send("game:minecraft:world:close", map[string]string{"world_id": opened.WorldID}) }()

	h := &session{cfg: cfg, srv: srv, rt: rt, world: opened.WorldID}
	return h.run(ctx, opened.Endpoint, opened.Ticket)
}

func findServer(cfg Config) (server, error) {
	var errs []string
	if cfg.Transport == "auto" || cfg.Transport == "nethernet" {
		n, _, err := bds.FindNethernet(cfg.DiscoveryAddr, 6*time.Second)
		if err == nil {
			return &nnServer{n: n}, nil
		}
		errs = append(errs, "nethernet: "+err.Error())
	}
	if cfg.Transport == "auto" || cfg.Transport == "raknet" {
		card, err := bds.PingRaknet(cfg.RaknetAddr, 3*time.Second)
		if err == nil {
			return newRakServer(cfg.RaknetAddr, card)
		}
		errs = append(errs, "raknet: "+err.Error())
	}
	return nil, errors.New("no BDS found (" + strings.Join(errs, "; ") + ")")
}

func openPayload(cfg Config, c *bds.Card, nethernet bool) map[string]any {
	p := map[string]any{
		"sender_id":     fmt.Sprint(c.GUID),
		"version_name":  c.VersionName,
		"friends_only":  cfg.FriendsOnly,
		"join_approval": true,
		// A dedicated server: needs relay access on the account; the world list tags it.
		"host_kind": "server",
	}
	if cfg.Password != "" {
		p["password"] = cfg.Password
	}
	if cfg.Description != "" {
		p["description"] = cfg.Description
	}
	if nethernet {
		p["world_type"] = 1
		p["bedrock_world"] = map[string]any{
			"server_name": c.ServerName, "level_name": c.LevelName, "player_count": c.Players,
			"max_player_count": c.MaxPlayers, "version": c.Protocol, "game_type": c.GameModeID,
			"editor_world": false, "transport_layer": 2, "hardcore": c.Hardcore, "unknown_field1": 8,
		}
		if c.Nonce != "" {
			p["nethernet_nonce"] = c.Nonce
		}
	} else {
		p["world_type"] = 2
		p["raknet_world"] = map[string]any{
			"edition": "MCPE", "server_name": c.ServerName, "protocol_version": c.Protocol,
			"player_count": c.Players, "max_player_count": c.MaxPlayers, "server_unique_id": fmt.Sprint(c.GUID),
			"version_name": c.VersionName, "level_name": c.LevelName, "game_mode": c.GameMode,
			"game_mode_id": c.GameModeID, "ipv4_port": c.Port, "ipv6_port": 0,
		}
	}
	return p
}

func waitFor(ctx context.Context, rt *realtime.Client, name string, d time.Duration) (realtime.Event, error) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return realtime.Event{}, ctx.Err()
		case <-timer.C:
			return realtime.Event{}, errors.New("no " + name + " in time")
		case ev, ok := <-rt.Events:
			if !ok {
				return realtime.Event{}, errors.New("realtime closed")
			}
			if ev.Name == name {
				return ev, nil
			}
			if ev.Name == "error" {
				return realtime.Event{}, backendError(ev.Data)
			}
		}
	}
}

// session is one published world.
type session struct {
	cfg   Config
	srv   server
	rt    *realtime.Client
	world string
	game  *wsgame.Conn
}

func (s *session) run(ctx context.Context, endpoint, ticket string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tickets := make(chan [2]string, 1)
	tickets <- [2]string{endpoint, ticket}
	gameDown := make(chan error, 1)
	cardTick := time.NewTicker(5 * time.Second)
	defer cardTick.Stop()
	last := [2]int32{-1, -1}
	if s.cfg.Guard {
		go func() {
			time.Sleep(5 * time.Second)
			_ = s.rt.Send("game:minecraft:guard:bot", map[string]any{"world_id": s.world, "enabled": true})
		}()
	}
	for {
		select {
		case <-ctx.Done():
			if s.game != nil {
				s.game.Leave()
				s.game.Close()
			}
			return ctx.Err()
		case t := <-tickets:
			g, err := wsgame.Dial(ctx, t[0], t[1])
			if err != nil {
				return fmt.Errorf("game link: %w", err)
			}
			s.game = g
			s.wireBack()
			log.Printf("[host] game link up (host peer %d)", g.HostPeer)
			go func() { gameDown <- g.Run(s.onGame) }()
		case err := <-gameDown:
			log.Printf("[host] game link down: %v; asking for a reconnect ticket", err)
			_ = s.rt.Send("game:minecraft:world:host_reconnect_request", map[string]string{"world_id": s.world})
		case ev, ok := <-s.rt.Events:
			if !ok {
				return errors.New("realtime socket closed")
			}
			if err := s.onRealtime(ev, tickets); err != nil {
				return err
			}
		case <-cardTick.C:
			c := s.srv.card()
			if c == nil {
				return errors.New("BDS stopped answering")
			}
			if (c.Players != last[0] || c.MaxPlayers != last[1]) && c.MaxPlayers != 0 {
				last = [2]int32{c.Players, c.MaxPlayers}
				_ = s.rt.Send("game:minecraft:world:playercount:update", map[string]any{
					"world_id": s.world, "player_count": c.Players, "max_player_count": c.MaxPlayers})
			}
		}
	}
}

func (s *session) wireBack() {
	g := s.game
	switch srv := s.srv.(type) {
	case *nnServer:
		srv.n.SetBack(func(peer uint64, msg []byte) {
			for _, seg := range segments(msg) {
				if g.SendData(peer, wsgame.ServerTypeNethernet, seg) != nil {
					return
				}
			}
		})
	case *rakServer:
		srv.game = g
	}
}

func (s *session) onRealtime(ev realtime.Event, tickets chan [2]string) error {
	switch ev.Name {
	case "game:minecraft:world:join_review":
		var r struct {
			WorldID       string          `json:"world_id"`
			UserID        json.RawMessage `json:"requester_user_id"` // a string; sent back as is
			Username      string          `json:"requester_username"`
			SocketID      string          `json:"requester_socket_id"`
			NeedsApproval bool            `json:"needs_approval"`
		}
		_ = json.Unmarshal(ev.Data, &r)
		if r.WorldID != s.world {
			return nil
		}
		if r.NeedsApproval && !s.cfg.ApproveAll {
			log.Printf("[host] %s is not on the whitelist: refused", r.Username)
			_ = s.rt.Send("game:minecraft:world:join_reject", map[string]any{
				"world_id": s.world, "requester_socket_id": r.SocketID, "reason": "not_whitelisted"})
			return nil
		}
		log.Printf("[host] admitting %s", r.Username)
		_ = s.rt.Send("game:minecraft:world:join_approve", map[string]any{
			"world_id": s.world, "requester_socket_id": r.SocketID, "requester_user_id": r.UserID})
	case "game:minecraft:world:host_reconnect_ticket":
		var t struct {
			WorldID  string `json:"world_id"`
			Ticket   string `json:"game_ticket"`
			Endpoint string `json:"game_endpoint"`
		}
		_ = json.Unmarshal(ev.Data, &t)
		if t.WorldID == s.world && t.Ticket != "" {
			select {
			case tickets <- [2]string{t.Endpoint, t.Ticket}:
			default:
			}
		}
	case "game:minecraft:world:close", "game:minecraft:world:close:success":
		var c struct {
			WorldID string `json:"world_id"`
		}
		_ = json.Unmarshal(ev.Data, &c)
		if c.WorldID == s.world {
			return errors.New("world closed by the backend")
		}
	case "game:minecraft:guard:report":
		log.Printf("[guard] %s", string(ev.Data))
	case "error":
		log.Printf("[host] backend error: %s", string(ev.Data))
		if strings.Contains(string(ev.Data), "not host of world") {
			return errors.New("world is gone")
		}
	}
	return nil
}

func (s *session) onGame(ev wsgame.Event) {
	switch ev.ID {
	case wsgame.IDPeerJoined:
		log.Printf("[host] guest peer %d joined (user %d)", ev.Peer, ev.User)
	case wsgame.IDPeerLeft:
		s.srv.drop(ev.Peer)
		log.Printf("[host] guest peer %d left", ev.Peer)
	case wsgame.IDPeerDataRelay:
		s.srv.fromGuest(ev.Peer, ev.Data)
	case wsgame.IDPeerSignalRelay:
		if ev.ServerType == wsgame.ServerTypeP2P {
			// No direct P2P from a server: the guest stays on the relay.
			_ = s.game.SendSignal(ev.Peer, wsgame.ServerTypeP2P, []byte(`{"t":"bye"}`))
			return
		}
		s.srv.signal(ev.Peer, ev.Data)
	case wsgame.IDError:
		log.Printf("[host] game server: %s", ev.Reason)
	}
}

// segments splits one NetherNet message into relay segments ([countdown] + ≤10 000 bytes).
func segments(msg []byte) [][]byte {
	const size = 10000
	if len(msg) == 0 {
		return nil
	}
	count := (len(msg) + size - 1) / size
	if count > 256 {
		return nil
	}
	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		end := min(len(msg), (i+1)*size)
		seg := make([]byte, 0, end-i*size+1)
		seg = append(seg, byte(count-1-i))
		seg = append(seg, msg[i*size:end]...)
		out = append(out, seg)
	}
	return out
}

// ---- NetherNet BDS ----

type nnServer struct{ n *bds.NethernetBDS }

func (s *nnServer) card() *bds.Card                    { return s.n.Card() }
func (s *nnServer) nethernet() bool                    { return true }
func (s *nnServer) fromGuest(peer uint64, data []byte) { s.n.FromGuest(peer, data) }
func (s *nnServer) drop(peer uint64)                   { s.n.Drop(peer) }
func (s *nnServer) close()                             { s.n.Close() }

// signal: the guest app relays its Minecraft's decrypted LAN discovery messages. A
// CONNECTREQUEST opens our own link to BDS (its SDP is the guest app's business);
// APP_WORLD_DISCONNECT drops it.
func (s *nnServer) signal(peer uint64, data []byte) {
	if len(data) >= 3 && data[0] == 0x87 && data[1] == 0x26 && data[2] == 0x09 {
		s.n.Drop(peer)
		return
	}
	if len(data) >= 32 && binary.LittleEndian.Uint16(data[2:4]) == 2 {
		n := int(binary.LittleEndian.Uint32(data[28:32]))
		if 32+n <= len(data) && strings.HasPrefix(string(data[32:32+n]), "CONNECTREQUEST ") {
			log.Printf("[nethernet] guest %d connecting", peer)
			s.n.Connect(peer)
		}
	}
}

// ---- RakNet BDS ----

type rakServer struct {
	addr  string
	relay *bds.RaknetRelay
	game  *wsgame.Conn
	last  *bds.Card
}

func newRakServer(addr string, card *bds.Card) (*rakServer, error) {
	s := &rakServer{addr: addr, last: card}
	relay, err := bds.NewRaknetRelay(addr, func(peer uint64, d []byte) {
		if g := s.game; g != nil {
			_ = g.SendData(peer, wsgame.ServerTypeRaknet, d)
		}
	})
	if err != nil {
		return nil, err
	}
	s.relay = relay
	return s, nil
}

func (s *rakServer) card() *bds.Card {
	if c, err := bds.PingRaknet(s.addr, 2*time.Second); err == nil {
		s.last = c
		return c
	}
	return nil
}
func (s *rakServer) nethernet() bool                    { return false }
func (s *rakServer) fromGuest(peer uint64, data []byte) { s.relay.FromGuest(peer, data) }
func (s *rakServer) signal(uint64, []byte)              {}
func (s *rakServer) drop(peer uint64)                   { s.relay.Drop(peer) }
func (s *rakServer) close()                             { s.relay.Close() }

// ErrNoRelayAccess: the account may not publish servers (relay access is granted by an
// Overlix admin). Retrying cannot help, so Run gives up.
var ErrNoRelayAccess = errors.New("this Overlix account has no relay access: ask an Overlix admin to grant it")

func backendError(data []byte) error {
	if strings.Contains(string(data), "relay_access_required") {
		return ErrNoRelayAccess
	}
	return fmt.Errorf("backend error: %s", string(data))
}
