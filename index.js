import express from "express";
import axios from "axios";
import { parse } from "csv-parse";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { createInterface as createReadlineInterface } from "node:readline";

import { Server as McpServer } from "@modelcontextprotocol/sdk/server/index.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Express app setup
const app = express();
app.set("trust proxy", true);
app.use(express.json({ limit: "2mb" }));

// Basic request logging
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// ---------------------
// CSV job utilities (shared with MCP tools)
// ---------------------
// Payload limiting
const MAX_TEXT_BYTES = (() => {
  const v = Number.parseInt(process.env.MCP_MAX_TEXT_BYTES || "180000", 10);
  return Number.isFinite(v) && v > 1024 ? v : 180000; // ~180 KB default
})();

function byteLengthUtf8(str) {
  return Buffer.byteLength(str ?? "", "utf8");
}

function truncateUtf8(str, maxBytes) {
  if (byteLengthUtf8(str) <= maxBytes) return { text: str, truncated: false };
  const buf = Buffer.from(str, "utf8");
  const sliced = buf.subarray(0, Math.max(0, maxBytes - 3));
  const text = sliced.toString("utf8");
  return { text: text + "\n…[truncated]", truncated: true };
}

async function readNdjsonPage(filePath, { lineOffset = 0, lineLimit = 500, maxBytes = MAX_TEXT_BYTES } = {}) {
  return await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    const rl = createReadlineInterface({ input: stream, crlfDelay: Infinity });
    let idx = -1;
    let returned = 0;
    let out = "";
    let overBytes = false;
    rl.on("line", (line) => {
      idx++;
      if (idx < lineOffset) return;
      if (returned >= lineLimit) return; // keep reading to know hasMore
      const candidate = (out ? out + "\n" : "") + line;
      if (byteLengthUtf8(candidate) > maxBytes) {
        overBytes = true;
        rl.close();
        return;
      }
      out = candidate;
      returned++;
    });
    rl.once("close", () => {
      const hasMore = overBytes || returned >= lineLimit;
      resolve({ text: out, returned, hasMore, offset: lineOffset });
    });
    rl.once("error", (err) => reject(err));
    stream.once("error", (err) => reject(err));
  });
}
const jobs = new Map();
const JOBS_ROOT = path.join(process.cwd(), "jobs");
if (!fs.existsSync(JOBS_ROOT)) fs.mkdirSync(JOBS_ROOT, { recursive: true });

function makeId() {
  return crypto.randomBytes(8).toString("hex");
}

function createJobRecord({ id, url, chunkSize }) {
  const dir = path.join(JOBS_ROOT, id);
  fs.mkdirSync(dir, { recursive: true });
  const record = {
    id,
    url,
    chunkSize,
    status: "pending", // pending | running | completed | failed
    createdAt: new Date().toISOString(),
    startedAt: null,
    finishedAt: null,
    totalRows: 0,
    files: [], // list of { name, path, rows }
    error: null,
  };
  jobs.set(id, record);
  return { record, dir };
}

async function startCsvJob({ url, chunkSize }) {
  const size = Number.isFinite(chunkSize) && chunkSize > 0 ? Math.floor(chunkSize) : 500;
  const id = makeId();
  const { record, dir } = createJobRecord({ id, url, chunkSize: size });

  (async () => {
    record.status = "running";
    record.startedAt = new Date().toISOString();

    let currentFileStream = null;
    let currentFilePath = null;
    let currentFileName = null;
    let fileIndex = 0;
    let rowsInChunk = 0;

    function openNewChunk() {
      fileIndex += 1;
      currentFileName = `chunk_${fileIndex}.ndjson`;
      currentFilePath = path.join(dir, currentFileName);
      currentFileStream = fs.createWriteStream(currentFilePath, { flags: "w" });
      rowsInChunk = 0;
      record.files.push({ name: currentFileName, path: currentFilePath, rows: 0 });
    }

    function closeChunk() {
      if (currentFileStream) {
        currentFileStream.end();
        currentFileStream = null;
        const f = record.files[record.files.length - 1];
        if (f) f.rows = rowsInChunk;
      }
    }

    try {
      const response = await axios.get(url, { responseType: "stream" });
      const sourceStream = response.data;
      const parser = parse({ columns: true, skip_empty_lines: true });

      openNewChunk();

      parser.on("data", (row) => {
        const line = JSON.stringify(row) + "\n";
        if (!currentFileStream) openNewChunk();
        currentFileStream.write(line);
        rowsInChunk += 1;
        record.totalRows += 1;
        if (rowsInChunk >= size) {
          closeChunk();
          openNewChunk();
        }
      });

      parser.on("end", () => {
        closeChunk();
        record.status = "completed";
        record.finishedAt = new Date().toISOString();
      });

      parser.on("error", (err) => {
        record.status = "failed";
        record.error = String(err.message || err);
        record.finishedAt = new Date().toISOString();
        try { closeChunk(); } catch {}
      });

      sourceStream.on("error", (err) => {
        record.status = "failed";
        record.error = String(err.message || err);
        record.finishedAt = new Date().toISOString();
        try { parser.destroy(); } catch {}
        try { closeChunk(); } catch {}
      });

      sourceStream.pipe(parser);
    } catch (err) {
      record.status = "failed";
      record.error = String(err.message || err);
      record.finishedAt = new Date().toISOString();
      try { fs.writeFileSync(path.join(dir, "error.txt"), String(err.stack || err)); } catch {}
    }
  })();

  return id;
}

async function exportCsvOnce({ url, mode = "json", limit = null, offset = 0 }) {
  const MAX_PROTECT = 5000;
  const response = await axios.get(url, { responseType: "stream" });
  const sourceStream = response.data;
  const parser = parse({ columns: true, skip_empty_lines: true });

  if (mode === "ndjson") {
  let nd = "";
    let rowIndex = 0;
    let sent = 0;

    return await new Promise((resolve, reject) => {
      sourceStream.pipe(parser);

      parser.on("data", (row) => {
        rowIndex++;
        if (rowIndex <= offset) return;
        const next = (nd ? nd + "\n" : "") + JSON.stringify(row);
        if (byteLengthUtf8(next) > MAX_TEXT_BYTES) {
          try { sourceStream.destroy(); } catch {}
          try { parser.destroy(); } catch {}
          const { text } = truncateUtf8(next, MAX_TEXT_BYTES);
          return resolve({ ndjson: text, returned: sent, offset, truncated: true });
        }
        nd = next;
        sent++;
        if (limit && sent >= limit) {
          try { sourceStream.destroy(); } catch {}
          try { parser.destroy(); } catch {}
          resolve({ ndjson: nd, returned: sent, offset });
        }
      });

      parser.on("end", () => resolve({ ndjson: nd, returned: sent, offset }));
      parser.on("error", reject);
      sourceStream.on("error", reject);
    });
  } else {
  const rows = [];
    let rowIndex = 0;
    let sent = 0;

    return await new Promise((resolve, reject) => {
      sourceStream.pipe(parser);

      parser.on("data", (row) => {
        rowIndex++;
        if (rowIndex <= offset) return;
        rows.push(row);
        sent++;
        if (limit && sent >= limit) {
          try { sourceStream.destroy(); } catch {}
          try { parser.destroy(); } catch {}
          resolve({ rows, offset, returned: rows.length, has_more: true });
        }
        if (!limit && rows.length > MAX_PROTECT) {
          try { sourceStream.destroy(); } catch {}
          try { parser.destroy(); } catch {}
          resolve({ rows, offset, returned: rows.length, protected: true });
        }
      });

      parser.on("end", () => {
        const has_more = (limit && rowIndex > offset + rows.length) || (!limit && rowIndex > rows.length + offset);
        resolve({ rows, offset, returned: rows.length, has_more });
      });

      parser.on("error", reject);
      sourceStream.on("error", reject);
    });
  }
}

// ---------------------
// MCP server factory and tool registration
// ---------------------
function createMcpServer() {
  const server = new McpServer(
    { name: "google-sheet-mcp", version: "0.1.0" },
    { capabilities: { tools: {} } }
  );

  const toolRegistry = new Map();
  const registerTool = (def, handler) => toolRegistry.set(def.name, { def, handler });

  server.setRequestHandler(ListToolsRequestSchema, async () => {
    const tools = Array.from(toolRegistry.values()).map((t) => t.def);
    return { tools };
  });

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params ?? {};
    console.log("[MCP] tools/call", name);
    const entry = toolRegistry.get(name);
    if (!entry) return { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
    try {
      const result = await entry.handler(args || {});
      if (!result || !Array.isArray(result.content)) {
        return { content: [{ type: "text", text: JSON.stringify(result ?? {}) }], isError: false };
      }
      return result;
    } catch (err) {
      return { content: [{ type: "text", text: `Tool error: ${String(err.message || err)}` }], isError: true };
    }
  });

  // Nudge clients to fetch tools after initialize
  server.oninitialized = async () => {
    try {
      console.log("[MCP] initialized; announcing tool list change");
      await server.sendToolListChanged();
    } catch (e) {
      console.warn("[MCP] sendToolListChanged failed", e);
    }
  };

  // Register tools
  registerTool(
    {
      name: "start_gsheets_csv_job",
      description: "Start an async job that reads a remote CSV (e.g., Google Sheets export) and writes NDJSON chunk files to a local jobs directory.",
      inputSchema: {
        type: "object",
        properties: {
          url: { type: "string", description: "HTTP URL to the CSV file" },
          chunkSize: { type: "number", description: "Rows per NDJSON chunk (default 500)" }
        },
        required: ["url"],
        additionalProperties: false
      }
    },
    async (input) => {
      try {
        const jobId = await startCsvJob({ url: input.url, chunkSize: input.chunkSize });
        const payload = { jobId, message: "Job accepted. Poll with get_job_status or list_job_results." };
        return { content: [{ type: "text", text: JSON.stringify(payload, null, 2) }] };
      } catch (err) {
        return { isError: true, content: [{ type: "text", text: `Failed to start job: ${String(err.message || err)}` }] };
      }
    }
  );

  registerTool(
    {
      name: "get_job_status",
      description: "Get metadata and status for a previously started job.",
      inputSchema: {
        type: "object",
        properties: { jobId: { type: "string" } },
        required: ["jobId"],
        additionalProperties: false
      }
    },
    async (input) => {
      const job = jobs.get(input.jobId);
      if (!job) return { isError: true, content: [{ type: "text", text: "Job not found" }] };
      const safeFiles = job.files.map((f) => ({ name: f.name, rows: f.rows }));
      const payload = {
        id: job.id,
        status: job.status,
        createdAt: job.createdAt,
        startedAt: job.startedAt,
        finishedAt: job.finishedAt,
        totalRows: job.totalRows,
        files: safeFiles,
        error: job.error,
      };
      return { content: [{ type: "text", text: JSON.stringify(payload, null, 2) }] };
    }
  );

  registerTool(
    {
      name: "list_job_results",
      description: "List NDJSON chunk files for a job.",
      inputSchema: {
        type: "object",
        properties: { jobId: { type: "string" } },
        required: ["jobId"],
        additionalProperties: false
      }
    },
    async (input) => {
      const job = jobs.get(input.jobId);
      if (!job) return { isError: true, content: [{ type: "text", text: "Job not found" }] };
      const files = job.files.map((f) => ({ name: f.name, rows: f.rows }));
      const payload = { id: job.id, status: job.status, totalRows: job.totalRows, files, error: job.error };
      return { content: [{ type: "text", text: JSON.stringify(payload, null, 2) }] };
    }
  );

  registerTool(
    {
      name: "get_job_file",
      description: "Return the contents of a specific NDJSON chunk file for a job.",
      inputSchema: {
        type: "object",
        properties: {
          jobId: { type: "string" },
          fileName: { type: "string", description: "e.g., chunk_1.ndjson" },
          encoding: { type: "string", enum: ["text", "base64"], description: "Return as plain text or base64", default: "text" },
          lineOffset: { type: "number", description: "Zero-based line offset for paging NDJSON", default: 0 },
          lineLimit: { type: "number", description: "Max lines to return for this page", default: 500 },
          maxBytes: { type: "number", description: "Hard ceiling on response size in bytes", default: MAX_TEXT_BYTES }
        },
        required: ["jobId", "fileName"],
        additionalProperties: false
      }
    },
    async (input) => {
      const job = jobs.get(input.jobId);
      if (!job) return { isError: true, content: [{ type: "text", text: "Job not found" }] };
      const dir = path.join(JOBS_ROOT, input.jobId);
      const safeName = path.basename(input.fileName);
      const filePath = path.join(dir, safeName);
      if (!fs.existsSync(filePath)) return { isError: true, content: [{ type: "text", text: "File not found" }] };

      const lineOffset = Number.isFinite(input.lineOffset) && input.lineOffset > 0 ? Math.floor(input.lineOffset) : 0;
      const lineLimit = Number.isFinite(input.lineLimit) && input.lineLimit > 0 ? Math.floor(input.lineLimit) : 500;
      const maxBytes = Number.isFinite(input.maxBytes) && input.maxBytes > 1024 ? Math.floor(input.maxBytes) : MAX_TEXT_BYTES;

      const page = await readNdjsonPage(filePath, { lineOffset, lineLimit, maxBytes });
      if (input.encoding === "base64") {
        const buf = Buffer.from(page.text, "utf8");
        const b64 = buf.toString("base64");
        const note = { file: safeName, lineOffset, returnedLines: page.returned, hasMore: page.hasMore, encoding: "base64" };
        return { content: [
          { type: "text", text: JSON.stringify(note) },
          { type: "text", text: b64 }
        ] };
      } else {
        const note = `{"file":"${safeName}","lineOffset":${lineOffset},"returnedLines":${page.returned},"hasMore":${page.hasMore}}\n`;
        const joined = note + page.text;
        const { text } = truncateUtf8(joined, maxBytes);
        return { content: [{ type: "text", text }] };
      }
    }
  );

  registerTool(
    {
      name: "gsheets_export_csv",
      description: "Fetch a CSV by URL and return either JSON rows (with limit/offset) or NDJSON text.",
      inputSchema: {
        type: "object",
        properties: {
          url: { type: "string" },
          mode: { type: "string", enum: ["json", "ndjson"], default: "json" },
          limit: { type: "number", nullable: true },
          offset: { type: "number", default: 0 }
        },
        required: ["url"],
        additionalProperties: false
      }
    },
    async (input) => {
      try {
        const mode = input.mode || "json";
        const userLimit = Number.isFinite(input.limit) ? Math.max(0, Math.floor(input.limit)) : null;
        const effectiveLimit = userLimit ?? (mode === "ndjson" ? 300 : 200);
        const result = await exportCsvOnce({ url: input.url, mode, limit: effectiveLimit, offset: input.offset || 0 });
        if (mode === "ndjson") {
          const { text } = truncateUtf8(result.ndjson || "", MAX_TEXT_BYTES);
          return { content: [{ type: "text", text }] };
        } else {
          let payload = result;
          let text = JSON.stringify(payload, null, 2);
          if (byteLengthUtf8(text) > MAX_TEXT_BYTES) {
            const rows = Array.isArray(payload.rows) ? payload.rows : [];
            let hi = rows.length;
            let lo = 0;
            let mid = Math.min(hi, 200);
            while (lo < hi) {
              const tryCount = mid;
              const t = JSON.stringify({ ...payload, rows: rows.slice(0, tryCount), returned: tryCount, has_more: true }, null, 2);
              if (byteLengthUtf8(t) <= MAX_TEXT_BYTES || tryCount <= 1) {
                text = t;
                break;
              }
              hi = tryCount - 1;
              mid = Math.floor((lo + hi) / 2);
            }
          }
          return { content: [{ type: "text", text }] };
        }
      } catch (err) {
        return { isError: true, content: [{ type: "text", text: `Export failed: ${String(err.message || err)}` }] };
      }
    }
  );

  // Targeted row finder to avoid large payloads for LLMs
  registerTool(
    {
      name: "gsheets_find_rows",
      description: "Stream a Google Sheets CSV and return only matching rows by email/name. Reduces payload vs exporting the whole sheet.",
      inputSchema: {
        type: "object",
        properties: {
          url: { type: "string", description: "CSV export URL (e.g., https://docs.google.com/.../export?format=csv)" },
          email: { type: "string", nullable: true, description: "Match on Email column" },
          name: { type: "string", nullable: true, description: "Match on Name column" },
          matchMode: { type: "string", enum: ["exact", "contains"], description: "Matching strategy", default: "exact" },
          caseInsensitive: { type: "boolean", description: "Case-insensitive match", default: true },
          select: { type: "array", items: { type: "string" }, nullable: true, description: "Columns to include in results" },
          maxMatches: { type: "number", description: "Max rows to return", default: 5 }
        },
        required: ["url"],
        additionalProperties: false
      }
    },
    async (input) => {
      const url = String(input.url);
      const wantEmail = input.email ? String(input.email) : null;
      const wantName = input.name ? String(input.name) : null;
      const matchMode = (input.matchMode === "contains" ? "contains" : "exact");
      const caseInsensitive = input.caseInsensitive !== false;
      const maxMatches = Number.isFinite(input.maxMatches) && input.maxMatches > 0 ? Math.floor(input.maxMatches) : 5;
      const select = Array.isArray(input.select) && input.select.length ? input.select.map(String) : null;

      if (!wantEmail && !wantName) {
        return { isError: true, content: [{ type: "text", text: "Provide at least one of: email or name" }] };
      }

      const normalize = (v) => String(v ?? "").trim();
      const normForCmp = (v) => {
        const s = normalize(v);
        return caseInsensitive ? s.toLowerCase() : s;
      };
      const cmp = (value, target) => {
        const a = normForCmp(value);
        const b = normForCmp(target);
        if (matchMode === "contains") return a.includes(b);
        return a === b;
      };

      const matches = [];
      let scanned = 0;

      try {
        const response = await axios.get(url, { responseType: "stream" });
        const sourceStream = response.data;
        const parser = parse({ columns: true, skip_empty_lines: true });

        const done = await new Promise((resolve, reject) => {
          sourceStream.pipe(parser);

          parser.on("data", (row) => {
            scanned++;
            const rowEmail = row.Email ?? row.email ?? row["E-mail"] ?? "";
            const rowName = row.Name ?? row.name ?? "";
            let ok = true;
            if (wantEmail) ok = ok && cmp(rowEmail, wantEmail);
            if (wantName) ok = ok && cmp(rowName, wantName);
            if (ok) {
              const out = select ? Object.fromEntries(select.map(k => [k, row[k]])) : row;
              matches.push(out);
              if (matches.length >= maxMatches) {
                try { sourceStream.destroy(); } catch {}
                try { parser.destroy(); } catch {}
                resolve(true);
              }
            }
          });

          parser.on("end", () => resolve(true));
          parser.on("error", (err) => reject(err));
          sourceStream.on("error", (err) => reject(err));
        });

        const payload = { scannedRows: scanned, returned: matches.length, matches };
        let text = JSON.stringify(payload, null, 2);
        if (byteLengthUtf8(text) > MAX_TEXT_BYTES) {
          const max = Math.max(1, Math.floor(matches.length / 2));
          const trimmed = { scannedRows: scanned, returned: max, matches: matches.slice(0, max) };
          text = JSON.stringify(trimmed, null, 2);
        }
        return { content: [{ type: "text", text }] };
      } catch (err) {
        return { isError: true, content: [{ type: "text", text: `Find failed: ${String(err.message || err)}` }] };
      }
    }
  );

  return server;
}

// ---------------------
// HTTP Streamable MCP over SSE
// ---------------------
class HttpSseTransport {
  constructor(res) {
    this.res = res;
    this.started = false;
    this.onmessage = undefined;
    this.onclose = undefined;
    this.onerror = undefined;
  }
  async start() {
    this.started = true;
  }
  async close() {
    try { this.res.end(); } catch {}
    this.onclose?.();
  }
  async send(message) {
    try {
      if (message && message.id !== undefined) {
        console.log("[MCP->HTTP]", typeof message === "object" ? message.method ?? "response" : "message");
      }
      const data = JSON.stringify(message);
      this.res.write(`data: ${data}\n\n`);
    } catch (err) {
      this.onerror?.(err);
    }
  }
  // Called by HTTP route when a client POSTs a message to the server
  receive(message) {
    try {
      if (message && message.method) {
        console.log("[HTTP->MCP]", message.method);
      }
      this.onmessage?.(message);
    } catch (err) {
      this.onerror?.(err);
    }
  }
}

// Per-request HTTP transport that returns the response inline (no SSE)
class HttpDirectTransport {
  constructor() {
    this.onmessage = undefined;
    this.onclose = undefined;
    this.onerror = undefined;
    this._response = null;
    this._done = null;
    this._donePromise = new Promise((resolve) => (this._done = resolve));
  }
  async start() {}
  async close() { this.onclose?.(); }
  async send(message) {
    // Capture the response (request handlers respond via send())
    this._response = message;
    this._done?.();
  }
  receive(message) {
    try {
      this.onmessage?.(message);
    } catch (err) {
      this.onerror?.(err);
    }
  }
  async waitResponse(timeoutMs = 10000) {
    const timer = new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), timeoutMs));
    await Promise.race([this._donePromise, timer]).catch(() => {});
    return this._response;
  }
}

// Session management
const sessions = new Map(); // sessionId -> { server, transport, res, keepAlive }

function createSseHeaders(res) {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
}

function startKeepAlive(res) {
  return setInterval(() => {
    try { res.write(": ping\n\n"); } catch {}
  }, 15000);
}

// Health endpoint
app.get("/health", (_req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

// Info endpoint
app.get(["/"], (_req, res) => {
  res.type("text").send("GoogleSheet MCP HTTP server is running. Use /mcp or /mcp/sse to connect.");
});

// SSE endpoint to establish MCP session (primary)
app.get(["/mcp/sse", "/sse"], async (req, res) => {
  const sessionId = (req.query.sessionId && String(req.query.sessionId)) || makeId();
  createSseHeaders(res);
  res.flushHeaders?.();
  // Recommend SSE retry
  res.write(`retry: 15000\n`);
  res.write(`event: session\n`);
  res.write(`data: ${JSON.stringify({ sessionId })}\n\n`);

  const transport = new HttpSseTransport(res);
  const server = createMcpServer();
  await server.connect(transport);

  const keepAlive = startKeepAlive(res);
  sessions.set(sessionId, { server, transport, res, keepAlive });

  req.on("close", () => {
    clearInterval(keepAlive);
    sessions.delete(sessionId);
    transport.close().catch(() => {});
  });
});

// Back-compat SSE endpoint used by some clients: GET /mcp (always SSE)
app.get("/mcp", async (req, res) => {
  const sessionId = (req.query.sessionId && String(req.query.sessionId)) || makeId();
  createSseHeaders(res);
  res.flushHeaders?.();
  res.write(`retry: 15000\n`);
  res.write(`event: session\n`);
  res.write(`data: ${JSON.stringify({ sessionId })}\n\n`);

  const transport = new HttpSseTransport(res);
  const server = createMcpServer();
  await server.connect(transport);

  const keepAlive = startKeepAlive(res);
  sessions.set(sessionId, { server, transport, res, keepAlive });

  req.on("close", () => {
    clearInterval(keepAlive);
    sessions.delete(sessionId);
    transport.close().catch(() => {});
  });
});

// Endpoint to receive client -> server JSON-RPC messages
app.post(["/mcp/messages/:sessionId", "/messages/:sessionId"], (req, res) => {
  const { sessionId } = req.params;
  const session = sessions.get(sessionId);
  if (!session) return res.status(404).json({ error: "Unknown session" });
  const message = req.body;
  if (!message) return res.status(400).json({ error: "Missing JSON body" });
  session.transport.receive(message);
  res.status(202).json({ ok: true });
});

// Back-compat message ingress: POST /mcp (uses header/query/body to infer session)
app.post("/mcp", (req, res) => {
  // accept a wide range of header names for session id
  const headers = req.headers || {};
  const requestedId =
    headers["x-session-id"] ||
    headers["x-mcp-session-id"] ||
    headers["x-mcp-session"] ||
    headers["mcp-session-id"] ||
    headers["mcp-session"] ||
    headers["sessionid"] ||
    req.query.sessionId ||
    req.query.session ||
    req.query.sid ||
    req.body?.sessionId;
  // cookie fallback
  const cookie = headers["cookie"];
  if (!requestedId && cookie) {
    const parts = String(cookie).split(/;\s*/);
    for (const p of parts) {
      const [k, v] = p.split("=");
      if (!k || !v) continue;
      const key = k.trim().toLowerCase();
      if (key === "sessionid" || key === "mcp-session" || key === "mcp-session-id") {
        req.headers["x-session-id"] = v; // normalize for logs
        break;
      }
    }
  }
  let sessionId = requestedId ? String(requestedId) : undefined;
  if (!sessionId) {
    if (sessions.size === 1) {
      // Use the only active session
      sessionId = sessions.keys().next().value;
    }
  }
  // If no session, handle as direct HTTP JSON-RPC (single request-response)
  // This helps clients that POST initialize before opening SSE.
  // If message has no id (notification), return 204.
  const message = req.body?.message ?? req.body;
  if (!message) return res.status(400).json({ error: "Missing JSON body" });

  if (!sessionId) {
    try {
      const transport = new HttpDirectTransport();
      const server = createMcpServer();
      server.connect(transport).then(() => {
        transport.receive(message);
      });
      transport.waitResponse(10000).then((response) => {
        if (!response) {
          // Likely a notification
          return res.status(204).end();
        }
        res.setHeader("Content-Type", "application/json");
        return res.status(200).send(JSON.stringify(response));
      }).catch((err) => {
        console.error("[HTTP] direct handling failed", err);
        res.status(500).json({ error: "Failed to handle request" });
      });
    } catch (e) {
      console.error("[HTTP] direct transport error", e);
      return res.status(500).json({ error: "Internal error" });
    }
    return;
  }

  const session = sessions.get(sessionId);
  if (!session) return res.status(404).json({ error: "Unknown session" });
  session.transport.receive(message);
  res.status(202).json({ ok: true });
});

// Optional: close a session explicitly
app.post(["/mcp/close/:sessionId", "/close/:sessionId"], async (req, res) => {
  const { sessionId } = req.params;
  const session = sessions.get(sessionId);
  if (!session) return res.status(404).json({ error: "Unknown session" });
  clearInterval(session.keepAlive);
  sessions.delete(sessionId);
  await session.transport.close();
  res.json({ closed: true });
});

// Keep the previous CSV utility endpoints for debugging/direct use
app.get("/jobs/:id/status", (req, res) => {
  const { id } = req.params;
  const job = jobs.get(id);
  if (!job) return res.status(404).json({ error: "Job not found" });
  const safeFiles = job.files.map((f) => ({ name: f.name, rows: f.rows }));
  res.json({ id: job.id, status: job.status, createdAt: job.createdAt, startedAt: job.startedAt, finishedAt: job.finishedAt, totalRows: job.totalRows, files: safeFiles, error: job.error });
});

app.get("/jobs/:id/result", (req, res) => {
  const { id } = req.params;
  const { file } = req.query;
  const job = jobs.get(id);
  if (!job) return res.status(404).json({ error: "Job not found" });
  const dir = path.join(JOBS_ROOT, id);

  if (file) {
    const safeName = path.basename(String(file));
    const filePath = path.join(dir, safeName);
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: "File not found" });
    res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
    fs.createReadStream(filePath).pipe(res);
    return;
  }

  const safeFiles = job.files.map((f) => ({ name: f.name, rows: f.rows, download: `/jobs/${id}/result?file=${encodeURIComponent(f.name)}` }));
  res.json({ id: job.id, status: job.status, totalRows: job.totalRows, files: safeFiles, error: job.error });
});

// Server start
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`HTTP Streamable MCP server listening on port ${PORT}`);
});
