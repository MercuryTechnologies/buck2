/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Writing the credentials file through BuildBuddy's CLI login: the browser signs in at
//! `<login_url>/cli-login`, which mints a personal API key and redirects to a listener on
//! localhost with `?token=<key>` (BuildBuddy's enterprise/app/cli_login). A key pasted on stdin
//! is taken too, for a browser on another machine. The key is never printed.

use std::io::BufRead;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

/// The URL the browser opens.
pub fn login_url(base: &str, port: u16, org: Option<&str>, workspace: &Path) -> String {
    let cli_url = format!("http://localhost:{port}");
    let mut url = format!(
        "{}/cli-login?cli_url={}&workspace={}",
        base.trim_end_matches('/'),
        encode(&cli_url),
        encode(&workspace.display().to_string())
    );
    if let Some(org) = org {
        url.push_str(&format!("&org={}", encode(org)));
    }
    url
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                Ok(b) => {
                    out.push(b);
                    i += 3;
                    continue;
                }
                Err(_) => out.push(b'%'),
            },
            b'+' => out.push(b' '),
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The `token` of an HTTP request line such as `GET /?token=abc HTTP/1.1`.
pub fn token_of_request_line(line: &str) -> Option<String> {
    let target = line.split_whitespace().nth(1)?;
    let query = target.split_once('?')?.1;
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("token="))
        .map(decode)
        .filter(|t| !t.trim().is_empty())
}

/// Answers one connection: a request carrying a token yields it, and the browser is sent back to
/// the login page with `complete=1` so that it shows the result; any other request is sent to
/// the login page.
fn answer(mut stream: TcpStream, login: &str) -> Option<String> {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let mut reader = std::io::BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let token = token_of_request_line(&line);
    let location = match token {
        Some(_) => format!("{login}&complete=1"),
        None => login.to_owned(),
    };
    let _ = write!(
        stream,
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    let _ = stream.flush();
    token
}

/// Waits for a key from either the listener or stdin, whichever comes first.
pub fn wait_for_key(
    listener: TcpListener,
    login: &str,
    stdin: Option<mpsc::Receiver<String>>,
    timeout: Duration,
) -> Result<String, String> {
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).map_err(|e| e.to_string())?;
                if let Some(key) = answer(stream, login) {
                    return Ok(key.trim().to_owned());
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e.to_string()),
        }
        if let Some(rx) = &stdin
            && let Ok(line) = rx.try_recv()
            && !line.trim().is_empty()
        {
            return Ok(line.trim().to_owned());
        }
        if std::time::Instant::now() > deadline {
            return Err("no key arrived; nothing changed".to_owned());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Writes `contents` to `path` with mode 600, through a temporary file and a rename, creating
/// the directory with mode 700.
pub fn write_secret(path: &Path, contents: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Opens `url` in a browser, quietly; the caller prints the URL either way.
pub fn open_browser(url: &str) -> bool {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    std::process::Command::new(program)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Lines from stdin, on a thread, for a key pasted instead of the browser's redirect.
pub fn stdin_lines() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = std::io::stdin().read_line(&mut buf);
        let _ = tx.send(buf);
    });
    rx
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn read_all(mut s: impl Read) -> String {
        let mut out = String::new();
        let _ = s.read_to_string(&mut out);
        out
    }

    #[test]
    fn the_login_url_carries_a_localhost_callback() {
        let url = login_url(
            "https://hub.example/",
            4242,
            Some("mercury"),
            Path::new("/w/mwb"),
        );
        assert_eq!(
            url,
            "https://hub.example/cli-login?cli_url=http%3A%2F%2Flocalhost%3A4242\
             &workspace=%2Fw%2Fmwb&org=mercury"
        );
    }

    #[test]
    fn a_token_is_read_from_the_request_line() {
        assert_eq!(
            token_of_request_line("GET /?token=ab%2Bc HTTP/1.1").as_deref(),
            Some("ab+c")
        );
        assert_eq!(token_of_request_line("GET /favicon.ico HTTP/1.1"), None);
        assert_eq!(token_of_request_line("GET /?token= HTTP/1.1"), None);
    }

    #[test]
    fn the_redirect_delivers_the_key_and_sends_the_browser_back() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
            write!(s, "GET /?token=KEY123 HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
            read_all(s)
        });
        let key = wait_for_key(
            listener,
            "https://hub/cli-login?x=1",
            None,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(key, "KEY123");
        let response = browser.join().unwrap();
        assert!(response.starts_with("HTTP/1.1 307"));
        assert!(response.contains("Location: https://hub/cli-login?x=1&complete=1"));
        assert!(!response.contains("KEY123"));
    }

    #[test]
    fn a_pasted_key_is_taken_too() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (tx, rx) = mpsc::channel();
        tx.send("  PASTED  \n".to_owned()).unwrap();
        assert_eq!(
            wait_for_key(listener, "u", Some(rx), Duration::from_secs(5)).unwrap(),
            "PASTED"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_secret_is_written_mode_600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("d/creds");
        write_secret(&path, "x").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(dir.path().join("d"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "x");
    }
}
