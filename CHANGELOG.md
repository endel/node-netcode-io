# Changelog

Notable changes to netcode-io. Versions follow [semantic versioning](https://semver.org);
`Unreleased` collects what has landed on `main` since the last release.

## 0.0.3

### Added

- Windows on x64, on either `websocket.thread`. `websocket.socketPath` isn't
  available there, and only a host named `node.exe` can load the addon.

### Changed

- Platform packages are published as `@netcode-io/<platform>`, in place of
  `netcode-io-<platform>`.

### Fixed

- `port: 0` tries another port when TCP can't have the one QUIC was given,
  where it used to throw at once.

## 0.0.2

### Added

- `websocket.socketPath` serves WebSockets and HTTP on a Unix socket instead of
  a TCP port.

## 0.0.1

First release: WebSocket and WebTransport on one port for Node.js 22 or later,
built on quic-zig, with plain HTTP alongside through an `http.Server`. Prebuilt
for Linux on x64 and arm64 and for Apple silicon.
