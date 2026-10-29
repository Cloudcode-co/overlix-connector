package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Cloudcode-co/overlix-connector/internal/bds"
)

// probe checks the local BDS without touching Overlix: finds it, prints its world card and,
// over NetherNet, opens one link and asks for network settings (what a joining guest does first).
func probe(args []string) {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	raknet := fs.String("raknet", "127.0.0.1:19132", "BDS RakNet address")
	discovery := fs.String("discovery", "", "NetherNet LAN discovery target")
	_ = fs.Parse(args)

	if n, card, err := bds.FindNethernet(*discovery, 6*time.Second); err == nil {
		fmt.Printf("nethernet: %+v\n", *card)
		got := make(chan []byte, 1)
		n.SetBack(func(_ uint64, msg []byte) {
			select {
			case got <- msg:
			default:
			}
		})
		n.Connect(1)
		// RequestNetworkSettings (id 193) with the server's protocol, as one relay segment.
		req := []byte{0, 6, 0xc1, 1, byte(card.Protocol >> 24), byte(card.Protocol >> 16), byte(card.Protocol >> 8), byte(card.Protocol)}
		n.FromGuest(1, req)
		select {
		case msg := <-got:
			fmt.Printf("nethernet: BDS answered %d bytes (first %x)\n", len(msg), msg[:min(8, len(msg))])
		case <-time.After(25 * time.Second):
			fmt.Println("nethernet: no answer from BDS")
			os.Exit(1)
		}
		n.Close()
		return
	} else {
		fmt.Println("nethernet:", err)
	}
	card, err := bds.PingRaknet(*raknet, 3*time.Second)
	if err != nil {
		log.Fatalf("raknet: %v", err)
	}
	fmt.Printf("raknet: %+v\n", *card)
}
