/**
 * Play Masr Pro - Real-Time Multi-Device Sync Server (Windows / Mac / Unlimited Mobile Phones)
 * Works 100% Offline over Local Wi-Fi (LAN) and Online!
 * Zero external dependencies required (runs on standard Node.js).
 */
const http = require('http');
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

const clients = new Set();

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

  // Real-time Server-Sent Events (SSE) stream for instant sync across all phones, Mac & Windows
  if (req.url === '/api/stream' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write(`data: ${JSON.stringify({ type: 'INIT', state: sharedState })}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  // State Sync Endpoint
  if (req.url === '/api/sync' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const parsed = JSON.parse(body);
        sharedState = parsed;
        fs.writeFileSync(DB_FILE, JSON.stringify(sharedState, null, 2), 'utf8');
        const payload = `data: ${JSON.stringify({ type: 'UPDATE', state: sharedState })}\n\n`;
        for (const client of clients) {
          client.write(payload);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, connectedClients: clients.size }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false }));
      }
    });
    return;
  }

  // Serve static files (index.html, manifest.json, sw.js)
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

server.listen(PORT, '0.0.0.0', () => {
  const ips = getLocalIpAddresses();
  console.log('==========================================================');
  console.log('🎮 Play Masr Pro - Real-Time Multi-Device Server Running!');
  console.log('==========================================================');
  console.log(`💻 على هذا الكمبيوتر (Windows / Mac): http://localhost:${PORT}`);
  ips.forEach(ip => {
    console.log(`📱 للفتح من أي موبايل أو تابلت أو جهاز آخر على نفس الواي فاي: http://${ip}:${PORT}`);
  });
  console.log('⚡ يعمل أونلاين وأوفلاين مع حفظ تلقائي فوري في قاعدة البيانات المحلية');
  console.log('==========================================================');
});
