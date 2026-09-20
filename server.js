/*!
 * 瞬传 · 离线投递中转服务器（零第三方依赖，仅需 Node.js >= 16）
 *
 * 作用：
 *   1. 静态托管同目录下的「瞬传_P2P_跨设备文件传输.html」（访问 http://本机IP:3000）
 *   2. 提供离线包裹的暂存 / 下载 / 打包 ZIP / 删除 API
 *
 * 运行：
 *   node server.js
 *
 * 环境变量（均可选）：
 *   PORT=3000                监听端口
 *   HOST=0.0.0.0             监听地址
 *   RELAY_TTL_HOURS=24       暂存文件保留时长（小时），到期自动删除
 *
 * 存储：
 *   ./relay-data/<取件码>/meta.json + 0.bin,1.bin,...
 */
'use strict';

const http = require('http');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const { pipeline } = require('stream/promises');
const { URL } = require('url');

const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const TTL_MS = Number(process.env.RELAY_TTL_HOURS || 24) * 3600 * 1000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'relay-data');
const HTML_NAME = '瞬传_P2P_跨设备文件传输.html';
const CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const CODE_RE = /^[A-Z0-9]{4,12}$/;
const MAX_JSON_BYTES = 2 * 1024 * 1024;

/* ---------------- 工具 ---------------- */
function log() {
  const args = Array.prototype.slice.call(arguments);
  args.unshift('[' + new Date().toISOString() + ']');
  console.log.apply(console, args);
}
function genCode() {
  let s = '';
  const a = cryptoRandom(8);
  for (let i = 0; i < 8; i++) s += CHARS[a[i] % CHARS.length];
  return s;
}
function cryptoRandom(n) {
  const b = require('crypto').randomBytes(n);
  return Array.prototype.slice.call(b);
}
function dirOf(code) { return path.join(DATA_DIR, code); }
function metaFileOf(code) { return path.join(dirOf(code), 'meta.json'); }
function dataFileOf(code, idx) { return path.join(dirOf(code), idx + '.bin'); }

async function readMeta(code) {
  const raw = await fsp.readFile(metaFileOf(code), 'utf8');
  return JSON.parse(raw);
}
async function writeMeta(code, meta) {
  const tmp = metaFileOf(code) + '.tmp';
  await fsp.writeFile(tmp, JSON.stringify(meta), 'utf8');
  await fsp.rename(tmp, metaFileOf(code));
}
function sendJson(res, status, obj) {
  const body = Buffer.from(JSON.stringify(obj), 'utf8');
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store'
  });
  res.end(body);
}
function readJsonBody(req, limit) {
  return new Promise(function (resolve, reject) {
    let size = 0;
    const chunks = [];
    req.on('data', function (c) {
      size += c.length;
      if (size > limit) { reject(new Error('body-too-large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', function () {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(new Error('bad-json')); }
    });
    req.on('error', reject);
  });
}
/* zip 内路径消毒：去掉盘符、..、绝对路径前缀 */
function safeZipPath(name, relPath) {
  let p = String(relPath || name || 'file').replace(/\\/g, '/');
  p = p.split('/').filter(function (s) { return s && s !== '.' && s !== '..'; }).join('/');
  if (!p) p = String(name || 'file').split('/').pop() || 'file';
  return p;
}

/* ---------------- CRC32（store zip 需要） ---------------- */
const CRC_TABLE = (function () {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c >>> 0;
  }
  return t;
})();
function crc32Buffer(buf, crc) {
  crc = (crc || 0) ^ 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) crc = CRC_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8);
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

/* ---------------- 路由处理 ---------------- */
async function handleInit(req, res) {
  const body = await readJsonBody(req, MAX_JSON_BYTES);
  const files = Array.isArray(body.files) ? body.files : null;
  if (!files || !files.length) return sendJson(res, 400, { error: 'files 不能为空' });
  if (files.length > 1000) return sendJson(res, 400, { error: '单次最多 1000 个文件' });
  const ttlHours = Number(body.ttlHours) > 0 && Number(body.ttlHours) <= 24 * 30
    ? Number(body.ttlHours) : Number(process.env.RELAY_TTL_HOURS || 24);
  const norm = files.map(function (f) {
    const size = Math.max(0, Math.floor(Number(f.size) || 0));
    const name = String(f.name || 'file').split('/').pop().split('\\').pop() || 'file';
    return {
      name: name.slice(0, 255),
      path: safeZipPath(name, f.path),
      size: size,
      mime: String(f.mime || 'application/octet-stream').slice(0, 200),
      crc: 0, received: false
    };
  });
  let code = genCode();
  for (let i = 0; i < 10; i++) {
    try { await fsp.mkdir(dirOf(code), { recursive: false }); break; }
    catch (e) { code = genCode(); }
  }
  const now = Date.now();
  const meta = {
    code: code, status: 'uploading',
    createdAt: now, expiresAt: now + ttlHours * 3600 * 1000,
    files: norm
  };
  await writeMeta(code, meta);
  log('INIT', code, norm.length + ' 个文件');
  sendJson(res, 200, { code: code, expiresAt: meta.expiresAt, ttlHours: ttlHours });
}

async function handleUpload(req, res, code, idxStr) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  const idx = Number(idxStr);
  let meta;
  try { meta = await readMeta(code); } catch (e) { return sendJson(res, 404, { error: '包裹不存在' }); }
  if (meta.status !== 'uploading') return sendJson(res, 409, { error: '包裹已结束上传' });
  const f = meta.files[idx];
  if (!f) return sendJson(res, 404, { error: '文件序号不存在' });
  const nameHeader = req.headers['x-name'] ? decodeURIComponent(req.headers['x-name']) : f.name;
  const pathHeader = req.headers['x-path'] ? decodeURIComponent(req.headers['x-path']) : f.path;
  f.name = String(nameHeader).split('/').pop().split('\\').pop().slice(0, 255) || f.name;
  f.path = safeZipPath(f.name, pathHeader);
  f.mime = String(req.headers['x-mime'] || f.mime).slice(0, 200);
  const expectSize = f.size;
  const tmp = dataFileOf(code, idx) + '.part';
  const ws = fs.createWriteStream(tmp);
  let received = 0, crc = 0, aborted = false;
  req.on('data', function (chunk) {
    if (aborted) return;
    received += chunk.length;
    crc = crc32Buffer(chunk, crc);
    if (received > expectSize) {
      aborted = true;
      ws.destroy();
      fsp.unlink(tmp).catch(function () {});
      sendJson(res, 413, { error: '字节数超过声明大小' });
      req.destroy();
    }
  });
  try {
    await pipeline(req, ws);
  } catch (e) {
    await fsp.unlink(tmp).catch(function () {});
    if (!aborted) log('UPLOAD FAIL', code, idx, e.message);
    return;
  }
  if (aborted) return;
  if (received !== expectSize) {
    await fsp.unlink(tmp).catch(function () {});
    return sendJson(res, 400, { error: '字节数不匹配，声明 ' + expectSize + ' 实收 ' + received });
  }
  await fsp.rename(tmp, dataFileOf(code, idx));
  f.received = true; f.crc = crc;
  await writeMeta(code, meta);
  log('UPLOADED', code, idx, f.path, received + 'B');
  sendJson(res, 200, { ok: true, idx: idx, crc: crc });
}

async function handleComplete(req, res, code) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  let meta;
  try { meta = await readMeta(code); } catch (e) { return sendJson(res, 404, { error: '包裹不存在' }); }
  const missing = meta.files.filter(function (f) { return !f.received; });
  if (missing.length) return sendJson(res, 409, { error: '还有 ' + missing.length + ' 个文件未上传完成' });
  meta.status = 'ready';
  await writeMeta(code, meta);
  const total = meta.files.reduce(function (s, f) { return s + f.size; }, 0);
  log('READY', code, meta.files.length + ' 个文件', total + 'B');
  sendJson(res, 200, { ok: true });
}

async function handleMeta(res, code) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  let meta;
  try { meta = await readMeta(code); } catch (e) { return sendJson(res, 404, { error: '包裹不存在' }); }
  if (meta.status !== 'ready') return sendJson(res, 404, { error: '包裹不存在或尚未上传完成' });
  sendJson(res, 200, {
    code: meta.code,
    createdAt: meta.createdAt,
    expiresAt: meta.expiresAt,
    totalSize: meta.files.reduce(function (s, f) { return s + f.size; }, 0),
    files: meta.files.map(function (f) {
      return { name: f.name, path: f.path, size: f.size, mime: f.mime };
    })
  });
}

async function handleFileDownload(req, res, code, idxStr) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  const idx = Number(idxStr);
  let meta;
  try { meta = await readMeta(code); } catch (e) { return sendJson(res, 404, { error: '包裹不存在' }); }
  if (meta.status !== 'ready') return sendJson(res, 404, { error: '包裹不存在' });
  const f = meta.files[idx];
  if (!f) return sendJson(res, 404, { error: '文件不存在' });
  const fp = dataFileOf(code, idx);
  let st;
  try { st = await fsp.stat(fp); } catch (e) { return sendJson(res, 410, { error: '文件已被清理' }); }
  const dlName = f.name.split('/').pop();
  res.writeHead(200, {
    'Content-Type': f.mime || 'application/octet-stream',
    'Content-Length': st.size,
    'Content-Disposition': "attachment; filename*=UTF-8''" + encodeURIComponent(dlName),
    'Cache-Control': 'no-store'
  });
  if (req.method === 'HEAD') return res.end();
  try { await pipeline(fs.createReadStream(fp), res); } catch (e) { /* 客户端取消下载 */ }
}

/* 以 store（不压缩）方式流式打 ZIP，支持中文 UTF-8 文件名 */
async function handleZip(req, res, code) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  let meta;
  try { meta = await readMeta(code); } catch (e) { return sendJson(res, 404, { error: '包裹不存在' }); }
  if (meta.status !== 'ready') return sendJson(res, 404, { error: '包裹不存在' });
  res.writeHead(200, {
    'Content-Type': 'application/zip',
    'Content-Disposition': "attachment; filename*=UTF-8''" + encodeURIComponent('瞬传-' + code + '.zip'),
    'Cache-Control': 'no-store'
  });
  if (req.method === 'HEAD') return res.end();

  function writeBuf(buf) {
    return new Promise(function (resolve, reject) {
      if (res.write(buf)) return resolve();
      res.once('drain', resolve);
      res.once('error', reject);
    });
  }
  function pumpFile(fp) {
    return new Promise(function (resolve, reject) {
      const rs = fs.createReadStream(fp);
      rs.on('data', function (c) { if (!res.write(c)) rs.pause(); });
      rs.on('drain', function () {});
      res.on('drain', function () { rs.resume(); });
      rs.on('end', resolve);
      rs.on('error', reject);
    });
  }

  try {
    const central = [];
    let offset = 0;
    for (let i = 0; i < meta.files.length; i++) {
      const f = meta.files[i];
      const fp = dataFileOf(code, i);
      await fsp.access(fp);
      const nameBuf = Buffer.from(f.path, 'utf8');
      const lh = Buffer.alloc(30);
      lh.writeUInt32LE(0x04034b50, 0);   // local file header signature
      lh.writeUInt16LE(20, 4);           // version needed
      lh.writeUInt16LE(0x0800, 6);       // flags: UTF-8 文件名
      lh.writeUInt16LE(0, 8);            // compression: store
      lh.writeUInt16LE(0, 10);           // mod time
      lh.writeUInt16LE(0x21, 12);        // mod date (1980-01-01)
      lh.writeUInt32LE(f.crc >>> 0, 14);
      lh.writeUInt32LE(f.size, 18);      // compressed size
      lh.writeUInt32LE(f.size, 22);      // uncompressed size
      lh.writeUInt16LE(nameBuf.length, 26);
      lh.writeUInt16LE(0, 28);           // extra length
      await writeBuf(Buffer.concat([lh, nameBuf]));
      offset += 30 + nameBuf.length;
      await pumpFile(fp);
      offset += f.size;
      central.push({ nameBuf: nameBuf, crc: f.crc >>> 0, size: f.size, offset: offset - f.size - 30 - nameBuf.length });
    }
    const cdStart = offset;
    let cdSize = 0;
    for (let i = 0; i < central.length; i++) {
      const e = central[i];
      const ch = Buffer.alloc(46);
      ch.writeUInt32LE(0x02014b50, 0);   // central file header signature
      ch.writeUInt16LE(20, 4);           // version made by
      ch.writeUInt16LE(20, 6);           // version needed
      ch.writeUInt16LE(0x0800, 8);       // flags
      ch.writeUInt16LE(0, 10);           // store
      ch.writeUInt16LE(0, 12); ch.writeUInt16LE(0x21, 14);
      ch.writeUInt32LE(e.crc, 16);
      ch.writeUInt32LE(e.size, 20);
      ch.writeUInt32LE(e.size, 24);
      ch.writeUInt16LE(e.nameBuf.length, 28);
      ch.writeUInt16LE(0, 30); ch.writeUInt16LE(0, 32);
      ch.writeUInt16LE(0, 34); ch.writeUInt16LE(0, 36);
      ch.writeUInt32LE(0, 38);           // external attrs
      ch.writeUInt32LE(e.offset, 42);    // local header offset
      await writeBuf(Buffer.concat([ch, e.nameBuf]));
      cdSize += 46 + e.nameBuf.length;
    }
    const eocd = Buffer.alloc(22);
    eocd.writeUInt32LE(0x06054b50, 0);
    eocd.writeUInt16LE(0, 4); eocd.writeUInt16LE(0, 6);
    eocd.writeUInt16LE(meta.files.length, 8);
    eocd.writeUInt16LE(meta.files.length, 10);
    eocd.writeUInt32LE(cdSize, 12);
    eocd.writeUInt32LE(cdStart, 16);
    eocd.writeUInt16LE(0, 20);
    await writeBuf(eocd);
    res.end();
    log('ZIP', code, meta.files.length + ' 个文件');
  } catch (e) {
    log('ZIP FAIL', code, e.message);
    try { res.destroy(); } catch (err) {}
  }
}

async function handleDelete(res, code) {
  if (!CODE_RE.test(code)) return sendJson(res, 400, { error: 'bad code' });
  try {
    await fsp.rm(dirOf(code), { recursive: true, force: true });
    log('DELETE', code);
    sendJson(res, 200, { ok: true });
  } catch (e) {
    sendJson(res, 500, { error: e.message });
  }
}

/* ---------------- 过期清理 ---------------- */
async function cleanupOnce() {
  let entries;
  try { entries = await fsp.readdir(DATA_DIR, { withFileTypes: true }); }
  catch (e) { return; }
  const now = Date.now();
  for (const ent of entries) {
    if (!ent.isDirectory() || !CODE_RE.test(ent.name)) continue;
    try {
      const meta = await readMeta(ent.name);
      if (meta.expiresAt && meta.expiresAt < now) {
        await fsp.rm(dirOf(ent.name), { recursive: true, force: true });
        log('EXPIRE', ent.name);
      }
    } catch (e) {
      // meta 损坏的孤立目录也清掉
      await fsp.rm(dirOf(ent.name), { recursive: true, force: true }).catch(function () {});
    }
  }
}

/* ---------------- HTTP 服务 ---------------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.png': 'image/png', '.ico': 'image/x-icon' };
const server = http.createServer(async function (req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,HEAD,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-name,x-path,x-size,x-mime');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  let u;
  try { u = new URL(req.url, 'http://localhost'); } catch (e) { return sendJson(res, 400, { error: 'bad url' }); }
  const p = u.pathname;

  try {
    if (p === '/' || p === '/index.html') {
      const html = await fsp.readFile(path.join(ROOT, HTML_NAME));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(html);
    }
    if (p === '/api/relay/ping' && req.method === 'GET') {
      return sendJson(res, 200, { ok: true, ttlHours: Number(process.env.RELAY_TTL_HOURS || 24) });
    }
    if (p === '/api/relay/init' && req.method === 'POST') return await handleInit(req, res);

    let m;
    if ((m = p.match(/^\/api\/relay\/([A-Z0-9]{4,12})\/file\/(\d+)$/))) {
      if (req.method === 'PUT') return await handleUpload(req, res, m[1], m[2]);
      if (req.method === 'GET' || req.method === 'HEAD') return await handleFileDownload(req, res, m[1], m[2]);
    }
    if ((m = p.match(/^\/api\/relay\/([A-Z0-9]{4,12})\/complete$/)) && req.method === 'POST')
      return await handleComplete(req, res, m[1]);
    if ((m = p.match(/^\/api\/relay\/([A-Z0-9]{4,12})\/meta$/)) && req.method === 'GET')
      return await handleMeta(res, m[1]);
    if ((m = p.match(/^\/api\/relay\/([A-Z0-9]{4,12})\/zip$/)) && (req.method === 'GET' || req.method === 'HEAD'))
      return await handleZip(req, res, m[1]);
    if ((m = p.match(/^\/api\/relay\/([A-Z0-9]{4,12})$/)) && req.method === 'DELETE')
      return await handleDelete(res, m[1]);

    sendJson(res, 404, { error: 'not found' });
  } catch (e) {
    log('ERROR', p, e.message);
    if (!res.headersSent) sendJson(res, 500, { error: '服务器内部错误' });
    else try { res.destroy(); } catch (err) {}
  }
});

fsp.mkdir(DATA_DIR, { recursive: true }).then(function () {
  server.listen(PORT, HOST, function () {
    log('瞬传中转服务器已启动');
    log('  本机访问： http://localhost:' + PORT);
    log('  暂存目录： ' + DATA_DIR);
    log('  保留时长： ' + (TTL_MS / 3600000) + ' 小时');
  });
  cleanupOnce();
  setInterval(cleanupOnce, 10 * 60 * 1000);
}).catch(function (e) {
  console.error('启动失败：', e);
  process.exit(1);
});
