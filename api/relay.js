/**
 * 瞬传 · Vercel 边缘函数（离线投递中转 API，免实名方案）
 *
 * 路由（本文件为 catch-all，匹配 /api/relay/ 下所有路径）：
 *   POST   /api/relay/init                 新建包裹 {files:[{name,path,size,mime}],ttlHours?}
 *   POST   /api/relay/:code/urls           签发分片直传令牌 {items:[{idx,parts:[{p,size}]}]}
 *   POST   /api/relay/:code/complete       上传完成 {files:[{name,path,size,mime,crc,parts:[{p,size}]}]}
 *   GET    /api/relay/:code/meta           包裹清单（uploading 状态返回 404；含每分片公开下载 URL）
 *   GET    /api/relay/:code/zip            流式打包 ZIP（store，CRC 由上传端预算，适合 ≤300MB 包裹）
 *   DELETE /api/relay/:code                删除包裹
 *   GET    /api/relay/ping                 健康检查
 *
 * 存储（Vercel Blob，免费 Hobby 版 1GB 存储 / 10GB 带宽 / 2000 次上传 / 月，超限只停用不扣费）：
 *   meta/<CODE>.json            包裹元数据（public，含随机分享码，等价凭证）
 *   pkg/<CODE>/<fidx>/p00000    文件分片（public；浏览器凭 clientToken 直传平台 API，字节不过函数）
 *
 * 客户端直传协议（与官方 @vercel/blob/client 一致）：
 *   PUT https://blob.vercel-storage.com/?pathname=<key>
 *   headers: authorization: Bearer <clientToken>, x-content-type: application/octet-stream, x-api-version: 9
 *
 * 说明：
 *   - 单文件下载由接收端浏览器直连分片公开 URL（桌面端流式写盘），函数只收发小 JSON 与 ZIP 流；
 *   - 无定时任务（Vercel 免费版无 cron），过期清理为惰性：每次 init 时顺带扫描删除过期包裹。
 */

import { put, del, list } from "@vercel/blob";
import { Readable } from "node:stream";

const PART_MAX = 20 * 1024 * 1024;   // 20MB 分片
const TTL_MS = 24 * 3600 * 1000;     // 默认保留 24 小时
const UPLOADING_GRACE_MS = 6 * 3600 * 1000; // uploading 超过 6 小时视为废弃
const MAX_FILES = 200;
const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CODE_RE = /^[A-Z0-9]{4,12}$/;

const encoder = new TextEncoder();

/* ---------------- 基础工具 ---------------- */

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,HEAD,OPTIONS",
    "Access-Control-Allow-Headers": "content-type,authorization",
    "Access-Control-Max-Age": "86400"
  };
}

function jsonResp(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...corsHeaders(), ...extra }
  });
}

function errResp(status, error) {
  return jsonResp({ error }, status);
}

function genCode() {
  const buf = new Uint8Array(8);
  crypto.getRandomValues(buf);
  let s = "";
  for (let i = 0; i < 8; i++) s += CODE_ALPHABET[buf[i] % CODE_ALPHABET.length];
  return s;
}

function partKey(code, fidx, p) {
  return `pkg/${code}/${fidx}/p${String(p).padStart(5, "0")}`;
}

function metaKey(code) {
  return `meta/${code}.json`;
}

/* ---------- Blob 读写封装（meta 一律 list 定位，不依赖 URL 预测） ---------- */

async function listAll(prefix) {
  const out = [];
  let cursor;
  do {
    const page = await list({ prefix, limit: 1000, cursor });
    for (const b of page.blobs || []) out.push(b);
    cursor = page.cursor;
  } while (cursor && page.hasMore !== false);
  return out;
}

async function readMeta(code) {
  const blobs = await listAll("meta/");
  const hit = blobs.find((b) => b.pathname === metaKey(code));
  if (!hit) return null;
  try {
    const res = await fetch(hit.url); // public，直接读
    if (!res.ok) return null;
    const m = await res.json();
    return m && m.code === code ? m : null;
  } catch (e) {
    return null;
  }
}

async function writeMeta(meta) {
  return put(metaKey(meta.code), JSON.stringify(meta), {
    access: "public",
    addRandomSuffix: false,
    contentType: "application/json",
    cacheControlMaxAge: 0
  });
}

async function deletePackage(code) {
  let removed = 0;
  for (const b of await listAll(`pkg/${code}/`)) {
    try { await del(b.url); removed++; } catch (e) { /* 下轮再删 */ }
  }
  const metas = await listAll("meta/");
  const m = metas.find((x) => x.pathname === metaKey(code));
  if (m) {
    try { await del(m.url); removed++; } catch (e) { /* ignore */ }
  }
  return removed;
}

async function cleanupExpired(now = Date.now()) {
  let checked = 0, deleted = 0;
  for (const b of await listAll("meta/")) {
    checked++;
    let m = null;
    try {
      const res = await fetch(b.url);
      if (!res.ok) continue;
      m = await res.json();
    } catch (e) { continue; }
    if (!m || !m.code) continue;
    const expired = (m.status === "ready" && m.expiresAt < now) ||
      (m.status === "uploading" && m.createdAt + UPLOADING_GRACE_MS < now);
    if (expired) {
      try { await deletePackage(m.code); deleted++; } catch (e) { /* 下轮再删 */ }
    }
  }
  return { deleted, checked, at: now };
}

/* ---------------- ZIP（store 模式，数据源为分片公开 URL） ---------------- */

function u16(n) { const b = new Uint8Array(2); new DataView(b.buffer).setUint16(0, n & 0xFFFF, true); return b; }
function u32(n) { const b = new Uint8Array(4); new DataView(b.buffer).setUint32(0, n >>> 0, true); return b; }

function concat(arrs) {
  let n = 0;
  for (const a of arrs) n += a.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const a of arrs) { out.set(a, o); o += a.length; }
  return out;
}

function dosDateTime(d = new Date()) {
  return {
    time: ((d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() / 2 | 0)) & 0xFFFF,
    date: (((d.getFullYear() - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate()) & 0xFFFF
  };
}

function localHeader(f, nameBytes, dd) {
  return concat([
    u32(0x04034b50), u16(20), u16(0x0800), u16(0),
    u16(dd.time), u16(dd.date),
    u32(f.crc >>> 0), u32(f.size), u32(f.size),
    u16(nameBytes.length), u16(0), nameBytes
  ]);
}

function centralHeader(f, nameBytes, offset, dd) {
  return concat([
    u32(0x02014b50), u16(20), u16(20), u16(0x0800), u16(0),
    u16(dd.time), u16(dd.date),
    u32(f.crc >>> 0), u32(f.size), u32(f.size),
    u16(nameBytes.length), u16(0), u16(0), u16(0), u16(0),
    u32(0), u32(offset >>> 0), nameBytes
  ]);
}

function eocd(n, cdSize, cdOffset) {
  return concat([
    u32(0x06054b50), u16(0), u16(0), u16(n), u16(n),
    u32(cdSize), u32(cdOffset >>> 0), u16(0)
  ]);
}

/** ZIP 内条目名：优先保留相对目录（path），并防 ZIP Slip 路径穿越 */
function zipEntryName(f) {
  const raw = (f.path && String(f.path)) || f.name;
  const segs = String(raw)
    .replace(/\\/g, "/")
    .split("/")
    .map((s) => s.trim().replace(/:/g, "_"))
    .filter((s) => s && s !== "." && s !== "..");
  return segs.join("/") || String(f.name).replace(/[\\/]/g, "_");
}

function zipSize(files) {
  let total = 0;
  const dd = dosDateTime();
  for (const f of files) {
    const nameLen = encoder.encode(zipEntryName(f)).length;
    total += 30 + nameLen + f.size + 46 + nameLen;
  }
  return total + 22;
}

/** 流式 ZIP：local header + 分片数据 + central directory + EOCD */
function zipStream(files) {
  const dd = dosDateTime();
  const central = [];
  let fi = 0, pi = 0, offset = 0, reader = null, state = "header";
  return new ReadableStream({
    async pull(controller) {
      while (true) {
        if (state === "data") {
          const { done, value } = await reader.read();
          if (!done) { controller.enqueue(value); return; }
          const f = files[fi - 1];
          if (pi < f.parts.length) {
            const res = await fetch(f.parts[pi].url);
            if (!res.ok || !res.body) throw new Error("分片获取失败");
            reader = res.body.getReader();
            pi++;
            continue;
          }
          reader = null;
          state = "header";
        }
        if (state === "header") {
          if (fi >= files.length) { state = "central"; continue; }
          const f = files[fi];
          const nb = encoder.encode(zipEntryName(f));
          const lh = localHeader(f, nb, dd);
          central.push(centralHeader(f, nb, offset, dd));
          offset += lh.length + f.size;
          controller.enqueue(lh);
          if (!f.parts.length) { fi++; state = "header"; continue; } /* 空文件无分片数据 */
          const res = await fetch(f.parts[0].url);
          if (!res.ok || !res.body) throw new Error("分片获取失败");
          reader = res.body.getReader();
          pi = 1;
          fi++;
          state = "data";
          return;
        }
        if (state === "central") {
          const cd = concat(central);
          controller.enqueue(cd);
          controller.enqueue(eocd(files.length, cd.length, offset));
          controller.close();
          return;
        }
      }
    }
  });
}

function downloadName(name) {
  const base = String(name).split("/").pop() || "download";
  return "attachment; filename*=UTF-8''" + encodeURIComponent(base);
}

/* ---------------- 业务校验 ---------------- */

function validateFileList(list) {
  if (!Array.isArray(list) || !list.length) throw new Error("文件清单为空");
  if (list.length > MAX_FILES) throw new Error("单次最多 " + MAX_FILES + " 个文件");
  return list.map((f, i) => {
    if (!f || typeof f.name !== "string" || !f.name) throw new Error("第 " + (i + 1) + " 个文件名非法");
    if (f.name.length > 512 || (f.path && String(f.path).length > 1024)) throw new Error("文件名过长");
    const size = Number(f.size);
    if (!Number.isInteger(size) || size < 0) throw new Error("文件大小非法：" + f.name);
    return {
      name: f.name.slice(0, 512),
      path: f.path ? String(f.path).slice(0, 1024) : f.name.slice(0, 512),
      size,
      mime: String(f.mime || "application/octet-stream").slice(0, 128)
    };
  });
}

function planParts(size, partSize) {
  const n = Math.max(1, Math.ceil(size / partSize));
  const out = [];
  for (let p = 0; p < n; p++) {
    const start = p * partSize;
    out.push({ p, size: Math.min(partSize, size - start) });
  }
  return out;
}

/* ---------------- 路由处理 ---------------- */

export async function handleRequest(request) {
  const url = new URL(request.url);
  const method = request.method.toUpperCase();
  const action = url.searchParams.get("action") || "";
  const code = url.searchParams.get("code") || "";

  if (method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  if (action === "ping" && method === "GET") {
    return jsonResp({ ok: true, service: "shunchuan-relay-vercel", time: Date.now() });
  }

  if (action === "init" && method === "POST") {
    const body = await request.json();
    const files = validateFileList(body.files);
    const ttlMs = Number(body.ttlHours) > 0 && Number(body.ttlHours) <= 30 * 24
      ? Number(body.ttlHours) * 3600 * 1000 : TTL_MS;
    try { await cleanupExpired(); } catch (e) { /* 清理失败不影响新建 */ }
    const now = Date.now();
    let code = null;
    for (let i = 0; i < 6; i++) {
      const candidate = genCode();
      const exist = await readMeta(candidate);
      if (!exist) { code = candidate; break; }
    }
    if (!code) return errResp(503, "取件码生成繁忙，请重试");
    const meta = {
      code, status: "uploading", createdAt: now, expiresAt: now + ttlMs,
      files: files.map((f) => ({ ...f, crc: null, parts: [] })),
      totalSize: files.reduce((s, f) => s + f.size, 0)
    };
    await writeMeta(meta);
    return jsonResp({ code, expiresAt: meta.expiresAt, partSize: PART_MAX });
  }

  if (!action || !CODE_RE.test(code)) return errResp(404, "路由不存在");

  /* POST /api/relay?action=urls&code=XXX —— 签发分片直传 clientToken */
  if (action === "urls" && method === "POST") {
    const meta = await readMeta(code);
    if (!meta) return errResp(404, "包裹不存在 debug=" + JSON.stringify(readMeta._debug || {}));
    if (meta.status !== "uploading") return errResp(409, "包裹已完成，不能继续上传");
    const body = await request.json();
    if (!body || !Array.isArray(body.items)) return errResp(400, "参数错误");
    const outItems = [];
    for (const it of body.items) {
      const idx = Number(it && it.idx);
      const f = meta.files[idx];
      if (!f || !Array.isArray(it.parts)) return errResp(400, "分片参数错误 idx=" + idx);
      const outParts = [];
      for (const pp of it.parts) {
        const p = Number(pp && pp.p);
        const size = Number(pp && pp.size);
        if (!Number.isInteger(p) || p < 0 || !Number.isInteger(size) || size < 0 || size > PART_MAX) {
          return errResp(400, "分片大小非法");
        }
        const expect = planParts(f.size, PART_MAX)[p];
        if (!expect || expect.size !== size) return errResp(400, "分片与文件大小不一致");
        const key = partKey(code, idx, p);
        const { generateClientTokenFromReadWriteToken } = await import("@vercel/blob/client");
        const token = await generateClientTokenFromReadWriteToken({
          pathname: key,
          allowedContentTypes: ["application/octet-stream"],
          maximumSizeInBytes: PART_MAX,
          addRandomSuffix: false,
          cacheControlMaxAge: 0,
          validUntil: Date.now() + 3600 * 1000
        });
        outParts.push({ p, size, pathname: key, token });
      }
      outItems.push({ idx, parts: outParts });
    }
    return jsonResp({ items: outItems });
  }

  /* POST /api/relay?action=complete&code=XXX */
  if (action === "complete" && method === "POST") {
    const meta = await readMeta(code);
    if (!meta) return errResp(404, "包裹不存在");
    if (meta.status !== "uploading") return errResp(409, "包裹已完成");
    const body = await request.json();
    const files = validateFileList(body && body.files);
    if (files.length !== meta.files.length) return errResp(400, "文件数量不一致");

    const uploaded = new Map((await listAll(`pkg/${code}/`)).map((b) => [b.pathname, b.url]));
    const completeFiles = [];
    for (let idx = 0; idx < files.length; idx++) {
      const declared = files[idx];
      const origin = meta.files[idx];
      if (declared.name !== origin.name || declared.size !== origin.size) {
        return errResp(400, "文件信息与初始化不一致");
      }
      const rawParts = body.files[idx].parts;
      if (!Array.isArray(rawParts) || !rawParts.length) return errResp(400, "缺少分片信息");
      const plan = planParts(origin.size, PART_MAX);
      if (rawParts.length !== plan.length) return errResp(400, "分片数量不一致：" + origin.name);
      const parts = [];
      for (let p = 0; p < plan.length; p++) {
        const rp = rawParts[p];
        if (!rp || Number(rp.p) !== p || Number(rp.size) !== plan[p].size) {
          return errResp(400, "分片信息不匹配：" + origin.name);
        }
        const key = partKey(code, idx, p);
        if (!uploaded.has(key)) return errResp(400, "分片未上传完成：" + origin.name + "#" + p);
        parts.push({ p, size: plan[p].size, url: uploaded.get(key) });
      }
      const crc = Number(body.files[idx].crc) >>> 0;
      if (!Number.isInteger(Number(body.files[idx].crc))) return errResp(400, "缺少校验值：" + origin.name);
      completeFiles.push({ ...origin, mime: declared.mime, crc, parts });
    }

    const ready = {
      ...meta,
      status: "ready",
      files: completeFiles,
      totalSize: completeFiles.reduce((s, f) => s + f.size, 0),
      completedAt: Date.now()
    };
    await writeMeta(ready);
    return jsonResp({ ok: true, code, expiresAt: ready.expiresAt, totalSize: ready.totalSize });
  }

  /* GET /api/relay?action=meta&code=XXX */
  if (action === "meta" && method === "GET") {
    const meta = await readMeta(code);
    if (!meta || meta.status !== "ready") return errResp(404, "包裹不存在或尚未上传完成");
    return jsonResp({
      code: meta.code,
      status: meta.status,
      createdAt: meta.createdAt,
      expiresAt: meta.expiresAt,
      totalSize: meta.totalSize,
      files: meta.files.map((f) => ({
        name: f.name, path: f.path, size: f.size, mime: f.mime,
        parts: (f.parts || []).map((x) => ({ p: x.p, size: x.size, url: x.url }))
      }))
    });
  }

  /* DELETE /api/relay?action=delete&code=XXX */
  if (action === "delete" && method === "DELETE") {
    const meta = await readMeta(code);
    if (!meta) return jsonResp({ ok: true, code, existed: false });
    const removed = await deletePackage(code);
    return jsonResp({ ok: true, code, removed });
  }

  /* GET/HEAD /api/relay?action=zip&code=XXX */
  if (action === "zip" && (method === "GET" || method === "HEAD")) {
    const meta = await readMeta(code);
    if (!meta || meta.status !== "ready") return errResp(404, "包裹不存在或尚未上传完成");
    const headers = {
      "Content-Type": "application/zip",
      "Content-Length": String(zipSize(meta.files)),
      "Content-Disposition": downloadName("瞬传-" + code + ".zip"),
      "Cache-Control": "private, no-transform",
      ...corsHeaders()
    };
    if (method === "HEAD") return new Response(null, { status: 200, headers });
    return new Response(zipStream(meta.files), { status: 200, headers });
  }

  return errResp(404, "路由不存在");
}


/* Vercel Node.js Runtime 兼容层：把 Node req/res 包装成 Web Request/Response */
export default async function handler(req, res) {
  const host = req.headers.host || "localhost";
  const url = "https://" + host + req.url;
  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (Array.isArray(v)) v.forEach(val => headers.append(k, val));
    else if (v != null) headers.set(k, String(v));
  }
  let bodyBuf;
  if (req.method !== "GET" && req.method !== "HEAD") {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    bodyBuf = Buffer.concat(chunks);
  }
  const request = new Request(url, {
    method: req.method || "GET",
    headers,
    body: bodyBuf && bodyBuf.length ? bodyBuf : undefined
  });
  try {
    const response = await handleRequest(request);
    res.statusCode = response.status;
    response.headers.forEach((v, k) => res.setHeader(k, v));
    if (response.body) {
      Readable.fromWeb(response.body).pipe(res);
    } else {
      res.end();
    }
  } catch (e) {
    res.statusCode = 500;
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.end(JSON.stringify({ error: String(e && e.message || e), stack: String(e && e.stack || "").slice(0, 2000) }));
  }
}
