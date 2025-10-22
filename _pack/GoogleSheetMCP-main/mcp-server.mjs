import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import axios from "axios";
import { parse } from "csv-parse";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import os from "os";

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
        nd += JSON.stringify(row) + "\n";
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
        encoding: { type: "string", enum: ["text", "base64"], description: "Return as plain text or base64", default: "text" }
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

    if (input.encoding === "base64") {
      const buf = fs.readFileSync(filePath);
      return { content: [{ type: "text", text: buf.toString("base64") }] };
    } else {
      const data = fs.readFileSync(filePath, "utf8");
      return { content: [{ type: "text", text: data }] };
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
      const result = await exportCsvOnce({ url: input.url, mode: input.mode || "json", limit: input.limit ?? null, offset: input.offset || 0 });
      if (input.mode === "ndjson") {
        return { content: [{ type: "text", text: result.ndjson }] };
      } else {
        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
      }
    } catch (err) {
      return { isError: true, content: [{ type: "text", text: `Export failed: ${String(err.message || err)}` }] };
    }
  }
);

// Start the stdio transport
const transport = new StdioServerTransport();
await server.connect(transport);

// Keep process alive, SDK handles stdio lifecycle
process.stdin.resume();
