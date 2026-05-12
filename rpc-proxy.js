#!/usr/bin/env node
/**
 * TVT RPC 代理 + 静态文件服务器
 * 
 * 功能：
 * 1. 提供 RPC 查询缓存（eth_call 等只读操作缓存 30 秒）
 * 2. 多节点自动容错
 * 3. 同时作为静态文件服务器托管前端页面
 * 
 * 使用：node rpc-proxy.js
 * 访问：http://localhost:3456
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

// ============ 配置 ============
const PORT = process.env.PORT || 3456;
const CACHE_TTL = 30_000; // 30秒缓存
const RPC_TIMEOUT = 12_000; // 单次 RPC 超时
const CACHE_CLEAN_INTERVAL = 60_000; // 1分钟清理过期缓存

// RPC 节点列表（按优先级排列）
const RPC_URLS = [
  'https://base-rpc.publicnode.com',
  'https://mainnet.base.org',
  'https://base.drpc.org',
  'https://1rpc.io/base',
  'https://base.meowrpc.com',
];

// 静态文件 MIME 映射
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif':  'image/gif',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.woff': 'font/woff',
  '.woff2':'font/woff2',
};

// 只读方法列表（可安全缓存）
const READ_METHODS = new Set([
  'eth_call',
  'eth_getBalance',
  'eth_getTransactionCount',
  'eth_blockNumber',
  'eth_chainId',
  'eth_gasPrice',
  'eth_estimateGas',
  'eth_getLogs',
  'eth_getTransactionReceipt',
  'eth_getBlockByNumber',
  'eth_getBlockByHash',
  'eth_getCode',
  'eth_getStorageAt',
  'eth_feeHistory',
  'net_version',
]);

// ============ 内存缓存 ============
const cache = new Map();
let cacheHits = 0;
let cacheMisses = 0;

function getCacheKey(body) {
  // eth_call 的完整参数作为 key
  if (body.method === 'eth_call' && body.params && body.params[0]) {
    return `eth_call:${body.params[0].to || ''}:${body.params[0].data || ''}`;
  }
  return `${body.method}:${JSON.stringify(body.params || [])}`;
}

function getFromCache(key) {
  const entry = cache.get(key);
  if (entry && Date.now() - entry.time < CACHE_TTL) {
    cacheHits++;
    return entry.data;
  }
  cache.delete(key);
  cacheMisses++;
  return null;
}

function setCache(key, data) {
  // 只缓存成功的响应
  if (data && !data.error) {
    cache.set(key, { data, time: Date.now() });
  }
}

// 定时清理过期缓存
setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of cache) {
    if (now - entry.time >= CACHE_TTL) cache.delete(key);
  }
  if (cache.size > 1000) {
    // 缓存过大时强制清理最老的一半
    const entries = [...cache.entries()].sort((a, b) => a[1].time - b[1].time);
    const toDelete = entries.slice(0, Math.floor(entries.length / 2));
    for (const [key] of toDelete) cache.delete(key);
  }
}, CACHE_CLEAN_INTERVAL);

// ============ RPC 调用（带多节点容错） ============
async function rpcCall(body, attempt = 0) {
  // 轮流使用节点
  const url = RPC_URLS[attempt % RPC_URLS.length];
  const maxAttempts = RPC_URLS.length * 2;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), RPC_TIMEOUT);

    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    clearTimeout(timer);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    // RPC 层面错误（如请求太频繁），尝试下一个节点
    if (data.error && data.error.code === -32005) {
      // rate limit, 换节点重试
      if (attempt + 1 < maxAttempts) {
        await new Promise(r => setTimeout(r, 200));
        return rpcCall(body, attempt + 1);
      }
    }

    return data;
  } catch (err) {
    if (attempt + 1 < maxAttempts) {
      await new Promise(r => setTimeout(r, 300));
      return rpcCall(body, attempt + 1);
    }
    throw err;
  }
}

// ============ HTTP 请求处理 ============
async function handleRequest(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // ---- RPC 代理 ----
  if (req.method === 'POST' && req.url === '/rpc') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      try {
        const jsonBody = JSON.parse(body);

        // 支持批量请求
        const requests = Array.isArray(jsonBody) ? jsonBody : [jsonBody];
        const results = [];

        for (const rpcReq of requests) {
          const key = getCacheKey(rpcReq);
          const cached = READ_METHODS.has(rpcReq.method) ? getFromCache(key) : null;

          if (cached) {
            results.push(cached);
          } else {
            const result = await rpcCall(rpcReq);
            if (READ_METHODS.has(rpcReq.method)) {
              setCache(key, result);
            }
            results.push(result);
          }
        }

        const responseData = Array.isArray(jsonBody) ? results : results[0];
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(responseData));

      } catch (err) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          jsonrpc: '2.0',
          id: null,
          error: { code: -32000, message: `Proxy error: ${err.message}` },
        }));
      }
    });
    return;
  }

  // ---- 健康检查 ----
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      cacheSize: cache.size,
      cacheHits,
      cacheMisses,
      hitRate: cacheHits + cacheMisses > 0
        ? (cacheHits / (cacheHits + cacheMisses) * 100).toFixed(1) + '%'
        : 'N/A',
    }));
    return;
  }

  // ---- 静态文件服务 ----
  const WEB_DIR = path.join(__dirname);
  let reqPath = req.url.split('?')[0];

  // 默认路由到 index.html
  if (reqPath === '/') reqPath = '/index.html';

  // 安全检查：防止目录穿越
  const fullPath = path.normalize(path.join(WEB_DIR, reqPath));
  if (!fullPath.startsWith(WEB_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  // 检查文件是否存在
  try {
    const stat = fs.statSync(fullPath);
    if (!stat.isFile()) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }

    const ext = path.extname(fullPath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    const content = fs.readFileSync(fullPath);
    res.writeHead(200, {
      'Content-Type': contentType,
      'Cache-Control': 'no-cache',
    });
    res.end(content);

  } catch (err) {
    // 未找到文件 → 返回 index.html（支持 SPA 路由）
    try {
      const idx = path.join(WEB_DIR, 'index.html');
      const content = fs.readFileSync(idx);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(content);
    } catch {
      res.writeHead(404);
      res.end('Not Found');
    }
  }
}

// ============ 启动服务器 ============
const server = http.createServer(handleRequest);
server.listen(PORT, '0.0.0.0', () => {
  console.log(`
╔═══════════════════════════════════╗
║  🌊 TVT Web Server + RPC Proxy   ║
╚═══════════════════════════════════╝

  地址: http://localhost:${PORT}
  缓存: ${CACHE_TTL / 1000}s TTL | ${RPC_URLS.length} 个 RPC 节点
  
  在浏览器打开 http://localhost:${PORT} 即可使用

  健康检查: http://localhost:${PORT}/health
  停止: Ctrl+C
`);
});
