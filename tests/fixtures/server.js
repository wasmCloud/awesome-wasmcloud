// Synthetic protocol peers. No Couchbase account or external service is used.
const http = require('node:http');
const http2 = require('node:http2');
const net = require('node:net');
const fs = require('node:fs');
const { once } = require('node:events');
const requests = [];
const frame = value => {
  const data = Buffer.from(value); const header = Buffer.alloc(5);
  header.writeUInt32BE(data.length, 1); return Buffer.concat([header, data]);
};
// Respect backpressure and stop allocating/writing as soon as the client aborts.
async function writeChunks(output, chunk, count) {
  const controller = new AbortController();
  const closed = () => controller.abort();
  output.once('close', closed);
  try {
    for (let index = 0; index < count && !output.destroyed; index++) {
      if (!output.write(chunk)) {
        await once(output, 'drain', { signal: controller.signal });
      }
    }
    if (!output.destroyed) output.end();
  } catch { /* Oversized replies are expected to be cancelled by the client. */ }
  finally { output.removeListener('close', closed); }
}
function grpc(stream, headers) {
  requests.push({method: headers[':method'], path: headers[':path'],
    timeout: headers['grpc-timeout'], authorization: headers.authorization});
  stream.on('error', () => {}); stream.resume();
  const mode = headers[':path'].split('/').pop();
  if (mode === 'RpcError') {
    stream.respond({':status': 200, 'content-type': 'application/grpc', 'grpc-status': '5',
      'grpc-message': 'missing'}, {endStream: true}); return;
  }
  const responseHeaders = {':status': mode === 'HttpError' ? 503 : 200,
    'content-type': mode === 'WrongContentType' ? 'text/html' : 'application/grpc'};
  if (mode === 'ConflictingStatus') responseHeaders['grpc-status'] = '0';
  stream.respond(responseHeaders, {waitForTrailers: true});
  stream.on('wantTrailers', () => stream.sendTrailers(mode === 'MissingStatus' ? {} :
    {'grpc-status': mode === 'InvalidStatus' ? '00' : mode === 'ConflictingStatus' ? '13' : '0'}));
  if (mode === 'HugeFrame') {
    const header = Buffer.alloc(5);
    header.writeUInt32BE(4 * 1024 * 1024 + 1, 1);
    stream.write(header);
    const timer = setTimeout(() => stream.end(), 10_000);
    stream.once('close', () => clearTimeout(timer));
    return;
  }
  if (mode === 'HugeBody') {
    void writeChunks(stream, frame(Buffer.alloc(4 * 1024 * 1024)), 9);
    return;
  }
  if (mode === 'FrameFlood') { stream.end(Buffer.alloc(5 * 1025)); return; }
  const values = {
    Empty: frame([]), Stream: Buffer.concat([frame('one'), frame('two')]),
    Malformed: Buffer.from([0, 0, 0, 0, 5, 10, 0]), ShortHeader: Buffer.from([0, 0]),
    Overflow: Buffer.from([0, 255, 255, 255, 255]), Compressed: Buffer.from([1, 0, 0, 0, 0]),
    InvalidFlag: Buffer.from([2, 0, 0, 0, 0]),
    TrailingGarbage: Buffer.concat([frame('ok'), Buffer.from([0])]),
  };
  const body = values[mode] || frame([10, 2, 111, 107]);
  if (mode === 'Slow') {
    let offset = 0;
    const timer = setInterval(() => {
      if (stream.destroyed) {clearInterval(timer); return;}
      stream.write(body.subarray(offset, ++offset));
      if (offset === body.length) {clearInterval(timer); stream.end();}
    }, 40);
  } else stream.end(body);
}
http2.createServer().on('stream', grpc).listen(18098, '0.0.0.0');
http2.createSecureServer({key: fs.readFileSync('/fixture/server.key'),
  cert: fs.readFileSync('/fixture/server.crt')}).on('stream', grpc).listen(18443, '0.0.0.0');
http.createServer((req, res) => {
  res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(requests));
}).listen(18099, '0.0.0.0');

http.createServer((req, res) => {
  requests.push({kind: 'data-api', method: req.method, path: req.url});
  const mode = req.url.split('/').pop();
  if (req.method === 'POST') {
    let body = ''; req.on('data', chunk => {body += chunk;});
    req.on('end', () => {
      const data = JSON.parse(body || '{}');
      if (data.statement === 'SELECT RAW "query-context"') {
        res.writeHead(200, {'content-type': 'application/json'});
        res.end(JSON.stringify({results: [data.query_context], status: 'success'}));
      } else if (data.statement === 'SELECT RAW "query-oversized"') {
        res.writeHead(200, {'content-type': 'application/json'});
        void writeChunks(res, Buffer.alloc(64 * 1024, 'x'), 33 * 16);
      } else if (['SELECT RAW "query-timeout"', 'SELECT RAW "query-timeout-http"'].includes(data.statement)) {
        res.writeHead(data.statement.includes('timeout-http') ? 500 : 200,
          {'content-type': 'application/json'});
        res.end(JSON.stringify({errors: [{code: 1080, msg: 'Timeout 1ms exceeded'}], status: 'timeout'}));
      } else {
        res.writeHead(503, {'content-type': 'application/json'});
        res.end('{"message":"query service temporarily unavailable"}');
      }
    }); return;
  }
  req.resume();
  if (mode === 'oversized') {
    res.writeHead(200, {'content-type': 'application/octet-stream'});
    void writeChunks(res, Buffer.alloc(64 * 1024, 'x'), 33 * 16);
    return;
  }
  res.writeHead(200, {'content-type': 'application/octet-stream',
    'content-length': 10, etag: '1234567890abcdef'});
  if (mode === 'complete') res.end('abcdefghij');
  else if (mode === 'slow') {
    const body = Buffer.from('abcdefghij'); let offset = 0;
    const timer = setInterval(() => {
      if (res.destroyed) {clearInterval(timer); return;}
      res.write(body.subarray(offset, ++offset));
      if (offset === body.length) {clearInterval(timer); res.end();}
    }, 40);
  } else {
    res.write('abc');
    setTimeout(() => res.destroy(), mode === 'timeout' ? 500 : 10);
  }
}).listen(18009, '0.0.0.0');

net.createServer(socket => {
  let pending = Buffer.alloc(0); const reads = new Map();
  socket.on('error', () => {});
  socket.on('data', chunk => {
    pending = Buffer.concat([pending, chunk]);
    while (pending.length >= 24) {
      const length = pending.readUInt32BE(8);
      if (pending.length < 24 + length) break;
      const head = pending.subarray(0, 24); const body = pending.subarray(24, 24 + length);
      pending = pending.subarray(24 + length);
      const op = head[1], keyLength = head.readUInt16BE(2), extrasLength = head[4];
      const key = body.subarray(extrasLength, extrasLength + keyLength).subarray(1).toString();
      let extras = Buffer.alloc(0), value = Buffer.alloc(0), cas = 0n, status = 0;
      if (op === 0x1f) value = body.subarray(extrasLength + keyLength);
      else if (op === 0xb5) value = Buffer.from('{"vBucketServerMap":{"vBucketMap":[[0]]}}');
      else if (op === 0xbb) extras = Buffer.alloc(12);
      else if (op === 0) {
        const count = (reads.get(key) || 0) + 1; reads.set(key, count);
        const version = key === 'expiry-race' && count > 1 ? 2 : 1;
        extras = Buffer.alloc(4); extras.writeUInt32BE(0x02000006);
        value = Buffer.from(JSON.stringify({version})); cas = version === 2 ? 222n : 111n;
      } else if (op === 0xa0) {
        if (key === 'expiry-error') status = 0x24;
        else {
          extras = Buffer.alloc(key === 'expiry-short' ? 3 : 20);
          if (extras.length >= 12) extras.writeUInt32BE(key === 'expiry-none' ? 0 : 2000000000, 8);
          cas = ['expiry-race', 'expiry-churn'].includes(key) ? 222n : 111n;
        }
      }
      if ([1, 2, 3].includes(op)) requests.push({kind: 'kv-write', keyLength});
      const reply = Buffer.alloc(24); reply[0] = 0x81; reply[1] = op; reply[4] = extras.length;
      reply.writeUInt16BE(status, 6); reply.writeUInt32BE(extras.length + value.length, 8);
      head.copy(reply, 12, 12, 16); reply.writeBigUInt64BE(cas, 16);
      if (key === 'kv-bad-header') { reply[4] = 20; reply.writeUInt32BE(0, 8); extras = value = Buffer.alloc(0); }
      if (key === 'kv-huge-header') { reply.writeUInt32BE(0xffffffff, 8); extras = value = Buffer.alloc(0); }
      if (key === 'kv-wrong-opcode') reply[1] = 1;
      if (key === 'kv-wrong-opaque') reply.writeUInt32BE((head.readUInt32BE(12) + 1) >>> 0, 12);
      socket.write(Buffer.concat([reply, extras, value]));
    }
  });
}).listen(11210, '0.0.0.0');
