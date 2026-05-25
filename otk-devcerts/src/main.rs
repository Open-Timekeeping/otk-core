//! One-shot dev cert generator for Open Timekeeping.
//!
//! Emits a complete TLS / mTLS bundle into a target directory in a
//! single command:
//!
//! ```text
//! <out>/
//!   server-ca.pem      ← trust root for producer-side `trust_roots`
//!   server-ca-key.pem  ← server CA private key (kept locally; not given to producers)
//!   server-cert.pem    ← leaf cert with SAN = "localhost" + 127.0.0.1 + ::1 by default
//!   server-key.pem     ← leaf private key, for `[listeners.tls] private_key`
//!   server-chain.pem   ← server-cert + server-ca, for `[listeners.tls] cert_chain`
//!   client-ca.pem      ← trust root for server-side `client_ca` (mTLS)
//!   client-ca-key.pem  ← client CA private key
//!   client-cert.pem    ← client leaf cert (CN = "otk-dev-client" by default)
//!   client-key.pem     ← client leaf private key
//! ```
//!
//! The two-tier shape (root CA + leaf, separately for server and
//! client) is what `rustls` + `webpki` expect; a single self-signed
//! cert used as both root and leaf produces `CaUsedAsEndEntity` at
//! handshake. Don't simplify it.
//!
//! **For dev use only.** Private keys are written world-readable;
//! the certs are short-lived (default 30 days) and the key pairs are
//! freshly random per run. Do not deploy these to production hosts
//! or commit the generated dir to a repo.
//!
//! # Usage
//!
//! ```text
//! otk-devcerts --out ./dev-certs
//! otk-devcerts --out ./dev-certs --server-san DNS:otk-node.lan,IP:192.168.1.50 --days 365
//! ```

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, SanType,
};

const USAGE: &str = "\
otk-devcerts -- generate a complete TLS + mTLS cert bundle for Open Timekeeping dev workflows.

Usage:
  otk-devcerts --out <DIR> [OPTIONS]

Options:
  --out <DIR>           Output directory. Created if it does not exist;
                        files inside are overwritten.
  --server-san <LIST>   Comma-separated server SANs. Each entry is
                        `DNS:<name>` or `IP:<addr>`. Default:
                        \"DNS:localhost,IP:127.0.0.1,IP:::1\".
  --server-cn <NAME>    Server leaf cert CN. Default: \"localhost\".
  --client-cn <NAME>    Client leaf cert CN. Default: \"otk-dev-client\".
  --days <N>            Validity period for every cert, in days. Default: 30.
  -h, --help            Print this help.
";

#[derive(Debug)]
struct Args {
    out: PathBuf,
    server_sans: Vec<SanType>,
    server_cn: String,
    client_cn: String,
    days: u32,
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!();
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = run(&args) {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    eprintln!("wrote dev cert bundle to {}", args.out.display());
    eprintln!();
    eprintln!("Server-side (timing-node `[listeners.tls]`):");
    eprintln!(
        "  cert_chain  = \"{}\"",
        args.out.join("server-chain.pem").display()
    );
    eprintln!(
        "  private_key = \"{}\"",
        args.out.join("server-key.pem").display()
    );
    eprintln!(
        "  client_ca   = \"{}\"  # for mTLS, omit for server-auth-only",
        args.out.join("client-ca.pem").display()
    );
    eprintln!();
    eprintln!("Producer-side (otk-sdk `[tls]`):");
    eprintln!(
        "  trust_roots = \"{}\"",
        args.out.join("server-ca.pem").display()
    );
    eprintln!(
        "  server_name = \"{}\"  # must match one of the server SANs",
        args.server_cn
    );
    eprintln!(
        "  client_cert = \"{}\"  # for mTLS",
        args.out.join("client-cert.pem").display()
    );
    eprintln!(
        "  client_key  = \"{}\"  # for mTLS",
        args.out.join("client-key.pem").display()
    );
    ExitCode::SUCCESS
}

fn parse_args() -> Result<Args, String> {
    let mut out: Option<PathBuf> = None;
    let mut server_sans_raw: Option<String> = None;
    let mut server_cn = String::from("localhost");
    let mut client_cn = String::from("otk-dev-client");
    let mut days: u32 = 30;

    let mut it = env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--out" => out = Some(PathBuf::from(it.next().ok_or("--out requires a value")?)),
            "--server-san" => {
                server_sans_raw = Some(it.next().ok_or("--server-san requires a value")?)
            }
            "--server-cn" => server_cn = it.next().ok_or("--server-cn requires a value")?,
            "--client-cn" => client_cn = it.next().ok_or("--client-cn requires a value")?,
            "--days" => {
                let v = it.next().ok_or("--days requires a value")?;
                days = v.parse::<u32>().map_err(|e| format!("--days: {e}"))?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    let out = out.ok_or("--out is required")?;
    let sans_str =
        server_sans_raw.unwrap_or_else(|| "DNS:localhost,IP:127.0.0.1,IP:::1".to_string());
    let server_sans = parse_sans(&sans_str)?;
    if server_sans.is_empty() {
        return Err("--server-san produced an empty SAN list".into());
    }

    Ok(Args {
        out,
        server_sans,
        server_cn,
        client_cn,
        days,
    })
}

fn parse_sans(raw: &str) -> Result<Vec<SanType>, String> {
    let mut out = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (kind, value) = entry
            .split_once(':')
            .ok_or_else(|| format!("SAN entry {entry:?} must be `DNS:<name>` or `IP:<addr>`"))?;
        match kind.to_ascii_uppercase().as_str() {
            "DNS" => out.push(SanType::DnsName(
                value
                    .try_into()
                    .map_err(|e| format!("SAN DNS value {value:?} rejected: {e}"))?,
            )),
            "IP" => {
                let ip: std::net::IpAddr = value
                    .parse()
                    .map_err(|e| format!("SAN IP value {value:?}: {e}"))?;
                out.push(SanType::IpAddress(ip));
            }
            other => return Err(format!("unknown SAN kind {other:?} in {entry:?}")),
        }
    }
    Ok(out)
}

struct Issued {
    cert: Certificate,
    key: KeyPair,
}

fn issue_ca(common_name: &str, days: u32) -> Result<Issued, Box<dyn std::error::Error>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    params.not_after = days_from_now(days);
    let key = KeyPair::generate()?;
    let cert = params.self_signed(&key)?;
    Ok(Issued { cert, key })
}

fn issue_leaf(
    issuer: &Issued,
    common_name: &str,
    sans: Vec<SanType>,
    ekus: Vec<ExtendedKeyUsagePurpose>,
    days: u32,
) -> Result<Issued, Box<dyn std::error::Error>> {
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.subject_alt_names = sans;
    params.is_ca = IsCa::NoCa;
    params.extended_key_usages = ekus;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    params.not_after = days_from_now(days);
    let key = KeyPair::generate()?;
    let cert = params.signed_by(&key, &issuer.cert, &issuer.key)?;
    Ok(Issued { cert, key })
}

fn days_from_now(days: u32) -> time::OffsetDateTime {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
        .saturating_add(u64::from(days).saturating_mul(86_400));
    time::OffsetDateTime::from_unix_timestamp(secs as i64)
        .expect("days * 86400 fits in i64 for any realistic input")
}

fn write_pem(dir: &Path, name: &str, contents: &str) -> std::io::Result<()> {
    let path = dir.join(name);
    fs::write(&path, contents)?;
    Ok(())
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(&args.out)?;

    // Server side: CA → leaf (with SANs, EKU = serverAuth).
    let server_ca = issue_ca("otk-dev-server-ca", args.days)?;
    let server_leaf = issue_leaf(
        &server_ca,
        &args.server_cn,
        args.server_sans.clone(),
        vec![ExtendedKeyUsagePurpose::ServerAuth],
        args.days,
    )?;

    // Client side: separate CA → leaf (EKU = clientAuth). The server's
    // [listeners.tls] client_ca trusts the client CA root, NOT the
    // server CA, so the two trust roots stay independent.
    let client_ca = issue_ca("otk-dev-client-ca", args.days)?;
    let client_leaf = issue_leaf(
        &client_ca,
        &args.client_cn,
        Vec::new(), // clients don't need SANs; CN identifies them
        vec![ExtendedKeyUsagePurpose::ClientAuth],
        args.days,
    )?;

    let server_cert_pem = server_leaf.cert.pem();
    let server_ca_pem = server_ca.cert.pem();
    let server_chain = format!("{server_cert_pem}{server_ca_pem}");

    write_pem(&args.out, "server-ca.pem", &server_ca_pem)?;
    write_pem(
        &args.out,
        "server-ca-key.pem",
        &server_ca.key.serialize_pem(),
    )?;
    write_pem(&args.out, "server-cert.pem", &server_cert_pem)?;
    write_pem(
        &args.out,
        "server-key.pem",
        &server_leaf.key.serialize_pem(),
    )?;
    write_pem(&args.out, "server-chain.pem", &server_chain)?;

    write_pem(&args.out, "client-ca.pem", &client_ca.cert.pem())?;
    write_pem(
        &args.out,
        "client-ca-key.pem",
        &client_ca.key.serialize_pem(),
    )?;
    write_pem(&args.out, "client-cert.pem", &client_leaf.cert.pem())?;
    write_pem(
        &args.out,
        "client-key.pem",
        &client_leaf.key.serialize_pem(),
    )?;

    Ok(())
}
