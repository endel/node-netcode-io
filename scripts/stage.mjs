// One npm package per platform, from the release workflow's artifacts:
//   node scripts/stage.mjs <artifacts dir> <out dir>
// Each holds a netcode_io.node that lib/binding.js finds by package name,
// at the version the main package's optionalDependencies ask for.
import fs from 'node:fs';
import path from 'node:path';

const [artifacts, out] = process.argv.slice(2);
const root = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const platforms = {
  'linux-x64': { os: 'linux', cpu: 'x64', libc: 'glibc' },
  'linux-arm64': { os: 'linux', cpu: 'arm64', libc: 'glibc' },
  'darwin-arm64': { os: 'darwin', cpu: 'arm64' },
  'win32-x64': { os: 'win32', cpu: 'x64' },
};

for (const [platform, t] of Object.entries(platforms)) {
  const name = `@netcode-io/${platform}`;
  if (root.optionalDependencies?.[name] !== root.version) {
    throw new Error(`package.json: optionalDependencies["${name}"] should be "${root.version}"`);
  }
  const dir = path.join(out, platform);
  fs.mkdirSync(dir, { recursive: true });
  fs.copyFileSync(path.join(artifacts, platform, 'netcode_io.node'), path.join(dir, 'netcode_io.node'));
  const rev = fs.readFileSync(path.join(artifacts, platform, 'quic-zig.rev'), 'utf8').trim();
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({
    name,
    version: root.version,
    description: `The ${platform} addon of netcode-io, built from quic-zig ${rev.slice(0, 7)}`,
    repository: root.repository,
    license: root.license,
    os: [t.os],
    cpu: [t.cpu],
    ...(t.libc ? { libc: [t.libc] } : {}),
    files: ['netcode_io.node'],
  }, null, 2) + '\n');
  fs.writeFileSync(path.join(dir, 'README.md'), `The ${platform} binary of [netcode-io](https://www.npmjs.com/package/netcode-io), which installs it for you.\n`);
  fs.copyFileSync(new URL('../LICENSE', import.meta.url), path.join(dir, 'LICENSE'));
  console.log(`${dir}: ${name}@${root.version}, quic-zig ${rev}`);
}
