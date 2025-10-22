import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import axios from "axios";
import { parse } from "csv-parse";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import os from "os";
import { createInterface as createReadlineInterface } from "node:readline";

// ---------------------
// Payload limiting
// ---------------------
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
    rl.once("error", (err) => {
      reject(err);
    });
    stream.once("error", (err) => {
      reject(err);
    });
  });
}

// ---------------------
// Simple job manager (ported from index.js)
// ---------------------
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

// Synchronous export utility (ported from index.js)
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
        // Guardrail: enforce byte ceiling as we build
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
// MCP Server
// ---------------------
const server = new Server(
  { name: "google-sheet-mcp", version: "0.1.0" },
  { capabilities: { tools: {} } }
);

// Simple tool registry
const toolRegistry = new Map();
function registerTool(def, handler) {
  toolRegistry.set(def.name, { def, handler });
}

// Tools/list handler
server.setRequestHandler(ListToolsRequestSchema, async (_request) => {
  const tools = Array.from(toolRegistry.values()).map((t) => t.def);
  return { tools };
});

// Tools/call handler
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params ?? {};
  const entry = toolRegistry.get(name);
  if (!entry) {
    return { content: [{ type: "text", text: `Unknown tool: ${name}` }], isError: true };
  }
  try {
    const result = await entry.handler(args || {});
    // Ensure result has content array
    if (!result || !Array.isArray(result.content)) {
      return { content: [{ type: "text", text: JSON.stringify(result ?? {}) }], isError: false };
    }
    return result;
  } catch (err) {
    return { content: [{ type: "text", text: `Tool error: ${String(err.message || err)}` }], isError: true };
  }
});

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
    if (!job) {
      return { isError: true, content: [{ type: "text", text: "Job not found" }] };
    }
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
    if (!job) {
      return { isError: true, content: [{ type: "text", text: "Job not found" }] };
    }
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
      const note = {
        file: safeName,
        lineOffset,
        returnedLines: page.returned,
        hasMore: page.hasMore,
        encoding: "base64",
      };
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
      // Safe defaults: cap rows when not specified
      const effectiveLimit = userLimit ?? (mode === "ndjson" ? 300 : 200);
      const result = await exportCsvOnce({ url: input.url, mode, limit: effectiveLimit, offset: input.offset || 0 });
      if (mode === "ndjson") {
        const { text } = truncateUtf8(result.ndjson || "", MAX_TEXT_BYTES);
        return { content: [{ type: "text", text }] };
      } else {
        let payload = result;
        let text = JSON.stringify(payload, null, 2);
        if (byteLengthUtf8(text) > MAX_TEXT_BYTES) {
          // If still too big, trim rows until it fits
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
          // Columns are case-sensitive; use exact header names from sheet
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
        // If somehow too big, trim matches
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

// Start the stdio transport
const transport = new StdioServerTransport();
await server.connect(transport);

// Keep process alive, SDK handles stdio lifecycle
process.stdin.resume();
