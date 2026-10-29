// overlix-connector publishes a Bedrock Dedicated Server on Overlix: players see it in
// their Overlix world list and join through the Overlix relay, with no port forwarding.
//
//	overlix-connector login                 sign in once (Overlix account of the server owner)
//	overlix-connector run [flags]           publish the BDS on this machine and relay players
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/term"

	"github.com/Cloudcode-co/overlix-connector/internal/account"
	"github.com/Cloudcode-co/overlix-connector/internal/host"
	"github.com/Cloudcode-co/overlix-connector/internal/realtime"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "login":
		login(os.Args[2:])
	case "run":
		run(os.Args[2:])
	case "probe":
		probe(os.Args[2:])
	case "version":
		fmt.Println("overlix-connector", version)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `overlix-connector: publish a Bedrock Dedicated Server on Overlix

  overlix-connector login [-token FILE]
  overlix-connector run   [-token FILE] [-transport auto|nethernet|raknet] [-raknet 127.0.0.1:19132]
                          [-discovery 255.255.255.255:7551] [-password P] [-description D]
                          [-friends-only] [-approve-all] [-guard] [-country VN]

Run "overlix-connector run -h" for every flag.`)
}

func defaultTokenFile() string {
	if v := os.Getenv("OVERLIX_CONNECTOR_TOKEN"); v != "" {
		return v
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		dir = "."
	}
	return filepath.Join(dir, "overlix-connector", "token.json")
}

func login(args []string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	tokenFile := fs.String("token", defaultTokenFile(), "where to keep the sign-in (a secret)")
	identity := fs.String("identity", env("OVERLIX_IDENTITY", account.DefaultIdentity), "identity API base")
	_ = fs.Parse(args)

	in := bufio.NewReader(os.Stdin)
	email := os.Getenv("OVERLIX_EMAIL")
	if email == "" {
		fmt.Print("Overlix email: ")
		line, _ := in.ReadString('\n')
		email = strings.TrimSpace(line)
	}
	password := os.Getenv("OVERLIX_PASSWORD")
	if password == "" {
		fmt.Print("Password: ")
		if term.IsTerminal(int(os.Stdin.Fd())) {
			b, _ := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			password = string(b)
		} else {
			line, _ := in.ReadString('\n')
			password = strings.TrimRight(line, "\r\n")
		}
	}
	if _, err := account.Login(context.Background(), *identity, *tokenFile, email, password); err != nil {
		log.Fatalf("sign-in failed: %v", err)
	}
	fmt.Println("Signed in. Saved to", *tokenFile)
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	tokenFile := fs.String("token", defaultTokenFile(), "sign-in file written by `login`")
	identity := fs.String("identity", env("OVERLIX_IDENTITY", account.DefaultIdentity), "identity API base")
	wsURL := fs.String("realtime", env("OVERLIX_REALTIME", realtime.DefaultURL), "realtime websocket URL")
	transport := fs.String("transport", env("OVERLIX_TRANSPORT", "auto"), "auto, nethernet (BDS transport=nethernet) or raknet")
	raknet := fs.String("raknet", env("OVERLIX_BDS_RAKNET", "127.0.0.1:19132"), "BDS RakNet address (server-port)")
	discovery := fs.String("discovery", env("OVERLIX_BDS_DISCOVERY", ""), "NetherNet LAN discovery target (default broadcast 255.255.255.255:7551)")
	password := fs.String("password", os.Getenv("OVERLIX_WORLD_PASSWORD"), "world password on Overlix (optional)")
	description := fs.String("description", os.Getenv("OVERLIX_WORLD_DESCRIPTION"), "world description (optional)")
	friendsOnly := fs.Bool("friends-only", false, "only your Overlix friends see and join the server")
	approveAll := fs.Bool("approve-all", false, "admit players the world's whitelist would hold for approval")
	guard := fs.Bool("guard", false, "ask for the worldGuard anti-spam bot")
	country := fs.String("country", env("OVERLIX_COUNTRY", "VN"), "country code shown with the world")
	_ = fs.Parse(args)

	acct, err := account.Open(*identity, *tokenFile)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("overlix-connector %s", version)
	err = host.Run(ctx, host.Config{
		RealtimeURL: *wsURL, Country: *country, Token: acct.Access, Transport: *transport,
		RaknetAddr: *raknet, DiscoveryAddr: *discovery, Password: *password, Description: *description,
		FriendsOnly: *friendsOnly, ApproveAll: *approveAll, Guard: *guard,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
