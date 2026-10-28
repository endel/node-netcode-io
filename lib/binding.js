'use strict';

const path = require('path');

// A local `zig build` wins, so a checkout runs what it just compiled.
function load() {
  const local = path.join(__dirname, '..', 'zig-out', 'lib', 'netcode_io.node');
  try {
    return require(local);
  } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') { throw e; }
  }

  const pkg = `netcode-io-${process.platform}-${process.arch}`;
  try {
    return require(`${pkg}/netcode_io.node`);
  } catch (e) {
    throw new Error(
      `netcode-io: no binary for ${process.platform}-${process.arch} (looked for ${pkg}).` +
      (process.platform === 'win32' ? ' Windows is not supported yet; use WSL.' : ''),
      { cause: e },
    );
  }
}

module.exports = load();
