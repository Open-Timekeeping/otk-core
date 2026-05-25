# otk-devcerts

One-shot dev cert generator for Open Timekeeping. Emits a complete TLS
+ mTLS bundle into a target directory so dev workflows have a working
cert set without an openssl ceremony.

> **Status: dev tooling only.** The generated material is short-lived
> (default 30 days), private keys are world-readable, and the
> generator runs every time you invoke it. **Don't deploy this output
> to production hosts. Don't commit a generated dir to a repo.**

## Why this exists

`rustls` + `webpki` (what `timing-node` and `otk-sdk` use) require a
real two-tier cert structure: a root CA and a separate leaf cert
signed by it. The naive `openssl req -x509` self-signed-in-one-call
recipe produces a cert that's both the CA and the leaf, which rustls
rejects at handshake with `CaUsedAsEndEntity`. For mTLS you also need
an independent client CA + client leaf. Writing the full ceremony out
in the README would be 10+ lines of openssl with subtle SAN /
extension flags that vary across openssl versions and platforms. One
binary that always emits the right shape is less rope.

## Usage

End-to-end demo flows that combine `otk-devcerts` with `otk-node` and
`otk-simulator` live at the workspace root: see the
[Getting started](../README.md#getting-started) section of the top-level
[README](../README.md). This page covers the generator's own flags and
its output shape.

```bash
# Default: server SANs = DNS:localhost,IP:127.0.0.1,IP:::1; 30-day validity.
# Matches the shipped `node-tls.toml` / `sim-start-tls.toml` out of the box.
cargo run -p otk-devcerts -- --out ./dev-certs

# Custom hostnames for a non-loopback dev deployment. Remember to update
# the producer's `[tls] server_name` (in sim-start-tls.toml) to match
# one of the SANs you pass here.
cargo run -p otk-devcerts -- \
    --out ./dev-certs \
    --server-san DNS:otk-node.lan,IP:192.168.1.50 \
    --server-cn otk-node.lan \
    --days 90
```

## Output

```
<out>/
  server-ca.pem        ← root CA cert; producers trust this
  server-ca-key.pem    ← root CA key (don't ship)
  server-cert.pem      ← leaf cert with the requested SANs
  server-key.pem       ← leaf private key
  server-chain.pem     ← server-cert + server-ca, in that order
  client-ca.pem        ← client root CA cert; the server trusts this for mTLS
  client-ca-key.pem    ← client root CA key (don't ship)
  client-cert.pem      ← client leaf cert (CN = otk-dev-client by default)
  client-key.pem       ← client leaf private key
```

## Dependencies

**Depends on:** [`rcgen`](https://crates.io/crates/rcgen) and
[`time`](https://crates.io/crates/time). No `otk-*` crate dep; this
tool is independent of every contract.

## License

Apache-2.0
