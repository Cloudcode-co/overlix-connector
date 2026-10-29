# overlix-connector

Publishes a Bedrock Dedicated Server (BDS) on Overlix. The server shows up in players'
Overlix world list, and they join through the Overlix relay. No port forwarding is needed.

It runs next to BDS as a small headless program. It is not a BDS plugin.

## Transports

- **NetherNet** (BDS `transport=nethernet`, the only transport from 1.26.50 on). The connector
  finds BDS with LAN discovery on UDP 7551 and opens one WebRTC link to it per player.
  Set `enable-lan-visibility=true` in `server.properties`.
- **RakNet** (older BDS). The connector pings `server-port` and opens one UDP socket per player.

`-transport auto` (the default) tries NetherNet first, then RakNet.

## Build

```
go build -o overlix-connector .
```

Go 1.24+.

## Use

```
overlix-connector login          # once: sign in with the server owner's Overlix account
overlix-connector probe          # check BDS is reachable (no Overlix traffic)
overlix-connector run            # publish and relay players
```

The sign-in is saved to `<config dir>/overlix-connector/token.json` (mode 0600). Override the
path with `-token` or `OVERLIX_CONNECTOR_TOKEN`. Treat the file like a password.

Useful `run` flags:

| Flag | Meaning |
| --- | --- |
| `-transport auto\|nethernet\|raknet` | BDS transport |
| `-raknet 127.0.0.1:19132` | BDS RakNet address (`server-port`) |
| `-discovery 255.255.255.255:7551` | NetherNet discovery target |
| `-password P` | world password on Overlix |
| `-description D` | world description |
| `-friends-only` | only your Overlix friends see and join |
| `-approve-all` | let in players the whitelist would hold for approval |
| `-guard` | ask for the worldGuard anti-spam bot |
| `-country VN` | country shown with the world |

Set `OVERLIX_DEBUG=1` for verbose logs.

## Notes

- Do not run the Overlix app or Minecraft on the BDS machine with NetherNet. They also bind
  UDP 7551, Windows hands BDS's replies to them, and the link to BDS times out.
- With `online-mode=false` BDS answers without an identity, which the connector accepts.
