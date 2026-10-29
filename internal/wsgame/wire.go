// Package wsgame speaks the Overlix /ws/game binary protocol as a world host.
//
// Frame: u32 LE body_size | u8 flags (bit0 = zstd) | uvarint packet id | payload.
// Payload integers are little-endian fixed width; strings and byte blobs carry a uvarint
// length. The connector only offers "no compression", so frames are never zstd.
package wsgame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// Packet ids (backend internal/modules/gameserver/packet/id.go).
const (
	IDClientHello     = 0x00
	IDServerHello     = 0x01
	IDReady           = 0x02
	IDPing            = 0x04
	IDPong            = 0x05
	IDDisconnect      = 0x06
	IDError           = 0x07
	IDJoinWorldAck    = 0x10
	IDPeerSignalRelay = 0x11
	IDPeerDataRelay   = 0x12
	IDPeerJoined      = 0x13
	IDPeerLeft        = 0x14
	IDWorldClosed     = 0x15
	IDPeerSignalSend  = 0x80
	IDPeerDataSend    = 0x81
	IDLeaveWorld      = 0x82

	// ProtocolVersion 1: the host never needs the guest resume packets of version 2.
	ProtocolVersion = 1

	ServerTypeNethernet = 1
	ServerTypeRaknet    = 2
	ServerTypeP2P       = 3
)

var errTruncated = errors.New("wsgame: truncated payload")

func encodeFrame(id uint64, payload []byte) []byte {
	var idBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(idBuf[:], id)
	body := 1 + n + len(payload)
	out := make([]byte, 4+body)
	binary.LittleEndian.PutUint32(out, uint32(body))
	out[4] = 0
	copy(out[5:], idBuf[:n])
	copy(out[5+n:], payload)
	return out
}

func decodeFrame(frame []byte) (id uint64, payload []byte, err error) {
	if len(frame) < 6 {
		return 0, nil, errTruncated
	}
	body := binary.LittleEndian.Uint32(frame)
	if int(body) != len(frame)-4 {
		return 0, nil, errTruncated
	}
	if frame[4]&1 != 0 {
		return 0, nil, errors.New("wsgame: compressed frame without negotiated zstd")
	}
	id, n := binary.Uvarint(frame[5:])
	if n <= 0 {
		return 0, nil, errTruncated
	}
	return id, frame[5+n:], nil
}

type writer struct{ bytes.Buffer }

func (w *writer) u8(v uint8)   { w.WriteByte(v) }
func (w *writer) u16(v uint16) { _ = binary.Write(&w.Buffer, binary.LittleEndian, v) }
func (w *writer) u64(v uint64) { _ = binary.Write(&w.Buffer, binary.LittleEndian, v) }
func (w *writer) blob(b []byte) {
	var lb [binary.MaxVarintLen64]byte
	w.Write(lb[:binary.PutUvarint(lb[:], uint64(len(b)))])
	w.Write(b)
}
func (w *writer) str(s string) { w.blob([]byte(s)) }

type reader struct{ r *bytes.Reader }

func newReader(b []byte) *reader { return &reader{bytes.NewReader(b)} }

func (r *reader) u8() (uint8, error) { return r.r.ReadByte() }
func (r *reader) u16() (v uint16, err error) {
	err = binary.Read(r.r, binary.LittleEndian, &v)
	return
}
func (r *reader) u32() (v uint32, err error) {
	err = binary.Read(r.r, binary.LittleEndian, &v)
	return
}
func (r *reader) u64() (v uint64, err error) {
	err = binary.Read(r.r, binary.LittleEndian, &v)
	return
}
func (r *reader) blob() ([]byte, error) {
	n, err := binary.ReadUvarint(r.r)
	if err != nil {
		return nil, err
	}
	if n > uint64(r.r.Len()) {
		return nil, errTruncated
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r.r, b)
	return b, err
}
func (r *reader) str() (string, error) {
	b, err := r.blob()
	return string(b), err
}
