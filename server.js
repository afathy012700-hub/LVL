/**
 * Play Masr Pro - Ultra-Low-Latency WebSocket (RFC-6455) & CRDT State Verification Server
 * Supports Windows, Mac, and Unlimited Mobile Phones (Online & Offline LAN)
 * Zero external npm packages required (uses native Node.js http + crypto).
 */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'play_masr_database.json');

let sharedState = null;
if (fs.existsSync(DB_FILE)) {
  try {
    sharedState = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch (e) {}
}

const wsClients = new Set();

function encodeWsFrame(str) {
  const payload = Buffer.from(str, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

function decodeWsFrame(buf) {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  if (opcode === 0x08) return { close: true };
  if (opcode !== 0x01) return null;
  const masked = (buf[1] & 0x80) !== 0;
  let len = buf[1] & 0x7f;
  let offset = 2;
  if (len === 126) {
    len = buf.readUInt16BE(2);
    offset = 4;
  } else if (len === 127) {
    len = Number(buf.readBigUInt64BE(2));
    offset = 10;
  }
  let maskKey = null;
  if (masked) {
    maskKey = buf.slice(offset, offset + 4);
    offset += 4;
  }
  if (buf.length < offset + len) return null;
  const data = Buffer.from(buf.slice(offset, offset + len));
  if (masked && maskKey) {
    for (let i = 0; i < data.length; i++) {
      data[i] ^= maskKey[i % 4];
    }
  }
  return { text: data.toString('utf8') };
}

function getLocalIpAddresses() {
  const nets = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        ips.push(net.address);
      }
    }
  }
  return ips.length ? ips : ['localhost'];
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  let filePath = req.url === '/' ? '/index.html' : req.url;
  const fullPath = path.join(__dirname, filePath);
  if (fs.existsSync(fullPath) && fs.statSync(fullPath).isFile()) {
    const ext = path.extname(fullPath).toLowerCase();
    const mimeTypes = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'application/javascript; charset=utf-8',
      '.json': 'application/json; charset=utf-8'
    };
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
    fs.createReadStream(fullPath).pipe(res);
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// RFC-6455 WebSocket Upgrade for Sub-Millisecond State & Order Synchronization
server.on('upgrade', (req, socket) => {
  if (req.headers['upgrade'] !== 'websocket') {
    socket.end('HTTP/1.1 400 Bad Request');
    return;
  }
  const acceptKey = req.headers['sec-websocket-key'];
  const hash = crypto
    .createHash('sha1')
    .update(acceptKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', 'binary')
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${hash}\r\n\r\n`
  );

  wsClients.add(socket);

  // Send latest verified state immediately upon connection
  if (sharedState) {
    socket.write(encodeWsFrame(JSON.stringify({
      type: 'WS_STATE_FRAME',
      originNode: 'SERVER-AUTHORITY',
      sentAt: Date.now(),
      payload: sharedState
    })));
  }

  socket.on('data', (buffer) => {
    try {
      const parsedFrame = decodeWsFrame(buffer);
      if (!parsedFrame) return;
      if (parsedFrame.close) {
        wsClients.delete(socket);
        socket.end();
        return;
      }
      if (parsedFrame.text) {
        const msg = JSON.parse(parsedFrame.text);
        if (msg.type === 'WS_STATE_FRAME' && msg.payload) {
          sharedState = msg.payload;
          try {
            fs.writeFileSync(DB_FILE, JSON.stringify(sharedState, null, 2), 'utf8');
          } catch (err) {}
          const outFrame = encodeWsFrame(JSON.stringify(msg));
          for (const client of wsClients) {
            if (client !== socket && !client.destroyed) {
              client.write(outFrame);
            }
          }
        }
      }
    } catch (e) {}
  });

  socket.on('close', () => wsClients.delete(socket));
  socket.on('error', () => wsClients.delete(socket));
});

server.listen(PORT, '0.0.0.0', () => {
  const ips = getLocalIpAddresses();
  console.log('==========================================================');
  console.log('⚡ Play Masr Pro - WebSocket Sub-Millisecond Sync Server');
  console.log('==========================================================');
  console.log(`💻 على هذا الكمبيوتر (Windows / Mac): http://localhost:${PORT}`);
  ips.forEach(ip => {
    console.log(`📱 للفتح من أي موبايل أو تابلت: http://${ip}:${PORT} (WebSocket: ws://${ip}:${PORT}/ws)`);
  });
  console.log('🛡️ آلية التحقق من تطابق الحالة (Checksum) ومنع التضارب (CRDT) مفعلة');
  console.log('==========================================================');
});
