import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import tls from 'node:tls';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

function run(command, args) {
  return execFileSync(command, args, { encoding: 'utf8', timeout: 180_000, stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

// Only the two public-image services are staged. No checkout or local
// credentials are mounted, and all published ports bind real host loopback.
export async function startCouchbaseTlsFixture() {
  const directory = await mkdtemp(join(tmpdir(), 'couchbase-tls-'));
  const project = `cb-tls-${randomUUID().slice(0, 8)}`;
  const composeFile = join(directory, 'compose.json');
  const compose = (...args) => run('docker', ['compose', '-p', project, '-f', composeFile, ...args]);
  const close = async () => {
    try { compose('down', '-v', '--remove-orphans'); }
    finally { await rm(directory, { recursive: true, force: true }); }
  };
  try {
    const definition = join(dirname(fileURLToPath(import.meta.url)), '../../couchbase/verification/docker-compose.yml');
    const config = JSON.parse(run('docker', ['compose', '-f', definition, 'config', '--format', 'json']));
    config.name = project;
    config.networks = { default: { name: `${project}-network` } };
    config.services = Object.fromEntries(['couchbase', 'couchbase-init'].map(name => [name, config.services[name]]));
    for (const [name, service] of Object.entries(config.services)) service.container_name = `${project}-${name}`;
    config.services.couchbase.ports = [8091, 11207, 18093].map(port => ({
      target: port, published: '0', host_ip: '127.0.0.1', protocol: 'tcp',
    }));
    await writeFile(composeFile, JSON.stringify(config));
    console.log('Starting isolated Couchbase TLS fixture');
    compose('up', '-d');
    let initialized = false;
    for (let attempt = 0; attempt < 120; attempt++) {
      const state = JSON.parse(run('docker', ['inspect', `${project}-couchbase-init`]))[0].State;
      if (state.Status === 'exited') {
        if (state.ExitCode !== 0) throw new Error(`Couchbase initialization failed: ${compose('logs', 'couchbase-init')}`);
        initialized = true;
        break;
      }
      await delay(1000);
    }
    if (!initialized) throw new Error('Couchbase initialization timed out');
    const container = `${project}-couchbase`;
    const bindings = JSON.parse(run('docker', ['inspect', container]))[0].NetworkSettings.Ports;
    const port = internal => Number(bindings[`${internal}/tcp`][0].HostPort);
    const cert = name => join(directory, name);
    run('openssl', ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-sha256',
      '-keyout', cert('ca.key'), '-out', cert('ca.pem'), '-subj', '/CN=Couchbase-example-test-CA',
      '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign']);
    run('openssl', ['req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', cert('pkey.key'),
      '-out', cert('node.csr'), '-subj', '/CN=host.wasmcloud.internal']);
    await writeFile(cert('node.ext'), [
      'subjectAltName=DNS:host.wasmcloud.internal,DNS:localhost,IP:127.0.0.1',
      'basicConstraints=critical,CA:FALSE', 'subjectKeyIdentifier=hash', 'authorityKeyIdentifier=keyid,issuer',
      'keyUsage=critical,digitalSignature,keyEncipherment', 'extendedKeyUsage=serverAuth', '',
    ].join('\n'));
    run('openssl', ['x509', '-req', '-in', cert('node.csr'), '-CA', cert('ca.pem'), '-CAkey', cert('ca.key'),
      '-CAcreateserial', '-out', cert('chain.pem'), '-days', '1', '-sha256', '-extfile', cert('node.ext')]);
    run('openssl', ['x509', '-in', cert('ca.pem'), '-outform', 'DER', '-out', cert('ca.der')]);
    const inbox = '/opt/couchbase/var/lib/couchbase/inbox';
    run('docker', ['exec', container, 'mkdir', '-p', `${inbox}/CA`]);
    for (const [name, destination] of [['ca.pem', 'CA/ca.pem'], ['chain.pem', 'chain.pem'], ['pkey.key', 'pkey.key']]) {
      run('docker', ['cp', cert(name), `${container}:${inbox}/${destination}`]);
    }
    run('docker', ['exec', container, 'chown', '-R', 'couchbase:couchbase', inbox]);
    for (const action of ['loadTrustedCAs', 'reloadCertificate']) {
      const response = await fetch(`http://127.0.0.1:${port(8091)}/node/controller/${action}`, {
        method: 'POST', headers: { authorization: `Basic ${Buffer.from('Administrator:password123').toString('base64')}` },
        signal: AbortSignal.timeout(15_000),
      });
      if (!response.ok) throw new Error(`Couchbase ${action}: HTTP ${response.status}: ${await response.text()}`);
      await response.arrayBuffer();
    }
    const ca = await readFile(cert('ca.pem'));
    let ready = false;
    for (let attempt = 0; attempt < 30; attempt++) {
      try {
        await new Promise((resolveProbe, reject) => {
          const socket = tls.connect({ host: '127.0.0.1', port: port(11207), servername: 'host.wasmcloud.internal', ca });
          const deadline = setTimeout(() => socket.destroy(new Error('Couchbase TLS probe timed out')), 3000);
          socket.once('error', reject);
          socket.once('close', () => clearTimeout(deadline));
          socket.once('secureConnect', () => { socket.end(); resolveProbe(); });
        });
        ready = true;
        break;
      } catch { await delay(1000); }
    }
    if (!ready) throw new Error('Couchbase TLS did not become ready');
    return { close, environment: {
      COUCHBASE_TLS_KV_PORT: String(port(11207)), COUCHBASE_TLS_QUERY_PORT: String(port(18093)),
      COUCHBASE_TLS_CA_DER: cert('ca.der'), COUCHBASE_TLS_CA_PEM: cert('ca.pem'),
    } };
  } catch (error) {
    await close().catch(cleanupError => console.error(`Couchbase cleanup: ${cleanupError.message}`));
    throw error;
  }
}
