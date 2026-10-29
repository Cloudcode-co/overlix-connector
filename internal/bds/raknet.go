// Package bds talks to the local Bedrock Dedicated Server: finds it, reads its world card,
// and carries each Overlix guest to it (RakNet datagrams or NetherNet messages).
package bds

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Card is what the Overlix world list shows.
type Card struct {
	ServerName  string
	LevelName   string
	Protocol    int32
	VersionName string
	Players     int32
	MaxPlayers  int32
	GameMode    string
	GameModeID  int32
	GUID        uint64
	Port        int
	// NetherNet only.
	Nonce    string
	Hardcore bool
}

var raknetMagic = []byte{0x00, 0xff, 0xff, 0x00, 0xfe, 0xfe, 0xfe, 0xfe, 0xfd, 0xfd, 0xfd, 0xfd, 0x12, 0x34, 0x56, 0x78}

// PingRaknet sends an unconnected ping to addr and parses the pong's MOTD.
func PingRaknet(addr string, timeout time.Duration) (*Card, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ping := make([]byte, 0, 33)
	ping = append(ping, 0x01)
	ping = binary.BigEndian.AppendUint64(ping, uint64(time.Now().UnixMilli()))
	ping = append(ping, raknetMagic...)
	var guid [8]byte
	_, _ = rand.Read(guid[:])
	ping = append(ping, guid[:]...)
	if _, err := conn.Write(ping); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return parsePong(buf[:n])
}

func parsePong(b []byte) (*Card, error) {
	// 0x1c | time u64 | server guid u64 | magic 16 | u16 len | motd
	if len(b) < 35 || b[0] != 0x1c {
		return nil, errors.New("not a RakNet pong")
	}
	guid := binary.BigEndian.Uint64(b[9:17])
	n := int(binary.BigEndian.Uint16(b[33:35]))
	if len(b) < 35+n {
		return nil, errors.New("pong truncated")
	}
	f := strings.Split(string(b[35:35+n]), ";")
	get := func(i int) string {
		if i < len(f) {
			return f[i]
		}
		return ""
	}
	num := func(i int) int64 {
		v, _ := strconv.ParseInt(get(i), 10, 64)
		return v
	}
	c := &Card{
		ServerName: get(1), Protocol: int32(num(2)), VersionName: get(3),
		Players: int32(num(4)), MaxPlayers: int32(num(5)), GUID: guid,
		LevelName: get(7), GameMode: get(8), GameModeID: int32(num(9)), Port: int(num(10)),
	}
	if c.Protocol == 0 || c.VersionName == "" {
		return nil, errors.New("pong carries no version (BDS answers empty pongs when transport=nethernet)")
	}
	return c, nil
}

// RaknetRelay keeps one UDP socket per guest towards BDS; replies go to back.
type RaknetRelay struct {
	target *net.UDPAddr
	back   func(peer uint64, datagram []byte)
	mu     sync.Mutex
	guests map[uint64]*net.UDPConn
}

func NewRaknetRelay(target string, back func(uint64, []byte)) (*RaknetRelay, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, err
	}
	return &RaknetRelay{target: addr, back: back, guests: map[uint64]*net.UDPConn{}}, nil
}

// FromGuest forwards one datagram, opening the guest's socket on its first one.
func (r *RaknetRelay) FromGuest(peer uint64, datagram []byte) {
	r.mu.Lock()
	conn := r.guests[peer]
	if conn == nil {
		var err error
		conn, err = net.DialUDP("udp", nil, r.target)
		if err != nil {
			r.mu.Unlock()
			return
		}
		r.guests[peer] = conn
		go r.pump(peer, conn)
	}
	r.mu.Unlock()
	_, _ = conn.Write(datagram)
}

func (r *RaknetRelay) pump(peer uint64, conn *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			r.Drop(peer)
			return
		}
		d := make([]byte, n)
		copy(d, buf[:n])
		r.back(peer, d)
	}
}

// Drop forgets a guest.
func (r *RaknetRelay) Drop(peer uint64) {
	r.mu.Lock()
	conn := r.guests[peer]
	delete(r.guests, peer)
	r.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Close drops every guest.
func (r *RaknetRelay) Close() {
	r.mu.Lock()
	peers := make([]uint64, 0, len(r.guests))
	for p := range r.guests {
		peers = append(peers, p)
	}
	r.mu.Unlock()
	for _, p := range peers {
		r.Drop(p)
	}
}
