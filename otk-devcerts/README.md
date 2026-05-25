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

```bash
# Default: server SANs = DNS:localhost,IP:127.0.0.1,IP:::1; 30-day validity
cargo run -p otk-devcerts -- --out ./dev-certs

# Or with custom hostnames for a non-loopback dev deployment
cargo run -p otk-devcerts -- \
    --out ./dev-certs \
    --server-san DNS:otk-node.lan,IP:192.168.1.50 \
    --server-cn otk-node.lan \
    --days 90
```

After running, the printed output tells you which paths to wire into
your `timing-node` and `otk-simulator` configs. Concretely:

```toml
# timing-node config (otk-node.toml)
[[listeners]]
transport = "tcp"
id = "tls-main"
bind_addr = "127.0.0.1:8463"

[listeners.tls]
cert_chain  = "./dev-certs/server-chain.pem"
private_key = "./dev-certs/server-key.pem"
client_ca   = "./dev-certs/client-ca.pem"  # optional, enables mTLS

# producer-simulated config (sim-start-tls.toml)
node_addr   = "127.0.0.1:8463"
producer_id = "sim-start"
# ... usual sim fields ...

[tls]
trust_roots = "./dev-certs/server-ca.pem"
server_name = "localhost"             # matches the server SAN
client_cert = "./dev-certs/client-cert.pem"  # both = mTLS, both omitted = server-only
client_key  = "./dev-certs/client-key.pem"
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
