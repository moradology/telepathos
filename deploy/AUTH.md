# Auth model: Tailscale is the identity layer

Telepathy's services assume they run inside your tailnet. Authentication,
encryption, and revocation are handled by Tailscale at the network layer —
**no application auth is required or recommended**.

## Why not tokens/OAuth

- Every device that can reach the services already holds a WireGuard identity
  issued to your account. The "who are you" question is answered before the
  first packet arrives.
- A shared bearer token would be strictly weaker (copyable) and more work
  (provisioning per device).
- OAuth/Google Sign-In would authenticate *inside* an already-authenticated
  tunnel — complexity without a new guarantee. (Revisit only if you ever need
  multi-user access from devices outside your tailnet.)

## Deployment (3090 box)

```sh
# telepathyd — bind loopback, expose via tailscale serve (TLS + identity)
TELEPATHY_BIND=127.0.0.1 TELEPATHY_API_PORT=8790 ./target/debug/telepathyd &
tailscale serve --bg --https=8443 http://127.0.0.1:8790

# node bridge — same pattern
TELEPATHY_PORT=8787 node server/dist/index.js &
tailscale serve --bg --https=8443 http://127.0.0.1:8787
# (or run one `tailscale serve` with path prefixes; see `tailscale serve --help`)
```

Phone-side connection fields (create a `tailscale` profile):
- bridge:      `wss://<machine-name>.<tailnet>.ts.net:8443`
- telepathyd:  `https://<machine-name>.<tailnet>.ts.net:8443`

Valid certificates, no cleartext policy exceptions needed on the phone
(the debug-only overlay exists for emulator/LAN-HTTP development).

## Hardening knobs

- **Tailscale ACLs** — scope which tailnet devices may reach ports 8787/8790.
  Default tailnet policy (all devices → all devices) is fine for personal use.
- **Optional bearer token** — `TELEPATHY_TOKEN` still works as a second factor
  (bridge hello + lane API header) if you ever expose services beyond the
  tailnet. Off by default.
- **Relay HMAC** — the Hermes gateway link keeps its per-gateway secret
  (contract §6.1); unaffected by this model.

## What auth is NOT for here

Multi-user. Telepathy is single-user by design (one human's earbuds, one
human's agents). If that ever changes, the answer is separate tailnet ACL
groups plus Hermes profile routing — still no application auth.
