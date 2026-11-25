# Changelog

Notable changes to netcode-io. Versions follow [semantic versioning](https://semver.org);
`Unreleased` collects what has landed on `main` since the last release.

## 0.0.5

### Added

- `server.stats().loopCpu` on Windows, and the QUIC thread is named
  `netcode-io` there for profilers, as on Linux and macOS.
- `bench/` runs on Windows: `wsload` and `wtload` build there and time round
  trips with `QueryPerformanceCounter`, and `run.mjs` reads the server's CPU
  from its cycle time. `run.mjs` also reports memory per connection, and
  takes `netcode@quic` for `websocket.thread: 'quic'`.

### Changed

- On Windows, with quic-zig's readiness reads: an idle WebSocket holds
  2.6 KB instead of 18.6, a message larger than 16 KiB is read in one go,
  and one of 32 KiB or more reaches a `transientMessages` handler without a
  copy, as on Linux.

### Fixed

- On Windows, events that arrived together could reach JS out of order, the
  first last: two HTTP requests on separate connections were handled the
  wrong way round in about half the runs.

## 0.0.4

### Added

- Prebuilt packages for Intel Macs, musl Linux (Alpine) on x64 and arm64, and
  Windows on arm64. `npm install` picks the right one.

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
