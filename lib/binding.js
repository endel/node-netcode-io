import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

// An ES module can't import a .node addon: Node loads those through require.
const require = createRequire(import.meta.url);

// A local `zig build` wins, so a checkout runs what it just compiled.
function load() {
  const local = fileURLToPath(new URL('../zig-out/lib/netcode_io.node', import.meta.url));
  try {
    return require(local);
  } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') { throw e; }
  }

  // npm installs the one whose `libc` matches. A package manager that ignores
  // the field leaves both, and the wrong one fails to load.
  const base = `@netcode-io/${process.platform}-${process.arch}`;
  const pkgs = process.platform === 'linux' ? [base, `${base}-musl`] : [base];
  let cause;
  for (const pkg of pkgs) {
    try {
      return require(`${pkg}/netcode_io.node`);
    } catch (e) {
      if (!cause || e.code !== 'MODULE_NOT_FOUND') { cause = e; }
    }
  }
  throw new Error(
    `netcode-io: no binary for ${process.platform}-${process.arch} (looked for ${pkgs.join(', ')}).`,
    { cause },
  );
}

export default load();
