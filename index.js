const express = require('express');
const axios = require('axios');
const { parse } = require('csv-parse');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');


const app = express();
app.use(express.json());

// Log every incoming request
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// Simple in-memory job store. For production, persist to a DB.
const jobs = new Map();
const JOBS_ROOT = path.join(__dirname, 'jobs');
if (!fs.existsSync(JOBS_ROOT)) fs.mkdirSync(JOBS_ROOT, { recursive: true });

function makeId() {
  return crypto.randomBytes(8).toString('hex');
}

function createJobRecord({ id, url, chunkSize }) {
  const dir = path.join(JOBS_ROOT, id);
  fs.mkdirSync(dir, { recursive: true });
  const record = {
    id,
    url,
    chunkSize,
    status: 'pending', // pending | running | completed | failed
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

// POST /jobs - start async job that streams CSV, writes NDJSON files per chunk
app.post('/jobs', (req, res) => {
  const { url, chunkSize } = req.body;
  if (!url) return res.status(400).json({ error: 'Missing url in body' });
  const size = parseInt(chunkSize, 10) || 500;
  const id = makeId();

  const { record, dir } = createJobRecord({ id, url, chunkSize: size });

  // Start processing asynchronously
  (async () => {
    record.status = 'running';
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
      currentFileStream = fs.createWriteStream(currentFilePath, { flags: 'w' });
      rowsInChunk = 0;
      record.files.push({ name: currentFileName, path: currentFilePath, rows: 0 });
    }

    function closeChunk() {
      if (currentFileStream) {
        currentFileStream.end();
        currentFileStream = null;
        // update rows count in metadata
        const f = record.files[record.files.length - 1];
        if (f) f.rows = rowsInChunk;
      }
    }

    try {
      const response = await axios.get(url, { responseType: 'stream' });
      const sourceStream = response.data;
      const parser = parse({ columns: true, skip_empty_lines: true });

      openNewChunk();

      parser.on('data', (row) => {
        // When a new row is parsed, write it as a JSON line
        const line = JSON.stringify(row) + '\n';
        if (!currentFileStream) openNewChunk();
        const ok = currentFileStream.write(line);
        rowsInChunk += 1;
        record.totalRows += 1;

        // If reached chunkSize, close and open new
        if (rowsInChunk >= size) {
          closeChunk();
          openNewChunk();
        }
      });

      parser.on('end', () => {
        closeChunk();
        record.status = 'completed';
        record.finishedAt = new Date().toISOString();
        console.log(`Job ${id} completed, rows=${record.totalRows}, files=${record.files.length}`);
      });

      parser.on('error', (err) => {
        console.error('CSV parser error for job', id, err);
        record.status = 'failed';
        record.error = String(err.message || err);
        record.finishedAt = new Date().toISOString();
        try { closeChunk(); } catch (e) {}
      });

      sourceStream.on('error', (err) => {
        console.error('Source stream error for job', id, err);
        record.status = 'failed';
        record.error = String(err.message || err);
        record.finishedAt = new Date().toISOString();
        try { parser.destroy(); } catch (e) {}
        try { closeChunk(); } catch (e) {}
      });

      // Pipe stream to parser
      sourceStream.pipe(parser);

    } catch (err) {
      console.error('Failed to start job', id, err);
      record.status = 'failed';
      record.error = String(err.message || err);
      record.finishedAt = new Date().toISOString();
      try { fs.writeFileSync(path.join(dir, 'error.txt'), String(err.stack || err)); } catch (e) {}
    }
  })();

  // Return job id immediately
  res.status(202).json({ jobId: id, statusUrl: `/jobs/${id}/status`, resultUrl: `/jobs/${id}/result` });
});

// GET /jobs/:id/status - get job metadata
app.get('/jobs/:id/status', (req, res) => {
  const { id } = req.params;
  const job = jobs.get(id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  // return metadata without exposing file system paths
  const safeFiles = job.files.map(f => ({ name: f.name, rows: f.rows }));
  res.json({ id: job.id, status: job.status, createdAt: job.createdAt, startedAt: job.startedAt, finishedAt: job.finishedAt, totalRows: job.totalRows, files: safeFiles, error: job.error });
});

// GET /jobs/:id/result - list available files or download a specific file
// Query: ?file=chunk_1.ndjson
app.get('/jobs/:id/result', (req, res) => {
  const { id } = req.params;
  const { file } = req.query;
  const job = jobs.get(id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  const dir = path.join(JOBS_ROOT, id);

  if (file) {
    // send specific file
    const safeName = path.basename(file.toString());
    const filePath = path.join(dir, safeName);
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'File not found' });
    res.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
    stream.on('error', (err) => {
      console.error('Error streaming file', err);
      if (!res.headersSent) res.status(500).json({ error: 'Failed to stream file' });
    });
    return;
  }

  // otherwise return list of files and job info
  const safeFiles = job.files.map(f => ({ name: f.name, rows: f.rows, download: `/jobs/${id}/result?file=${encodeURIComponent(f.name)}` }));
  res.json({ id: job.id, status: job.status, totalRows: job.totalRows, files: safeFiles, error: job.error });
});

// Keep the original single-request CSV endpoint for convenience (but it's streaming and protected)
app.post('/gsheets_export_csv', async (req, res) => {
  const { url } = req.body;
  const mode = (req.query.mode || 'json').toLowerCase(); // 'json' or 'ndjson'
  const limit = req.query.limit ? parseInt(req.query.limit, 10) : null;
  const offset = req.query.offset ? parseInt(req.query.offset, 10) : 0;
  const MAX_PROTECT = 5000; // safety cap when no limit provided

  if (!url) {
    return res.status(400).json({ error: 'Missing url in request body' });
  }

  try {
    // Stream the CSV from the remote URL so we don't buffer the whole file in memory
    const response = await axios.get(url, { responseType: 'stream' });
    const sourceStream = response.data;
    const parser = parse({ columns: true, skip_empty_lines: true });

    let rowIndex = 0; // absolute row count
    let sent = 0;     // rows returned after offset
    let ended = false;

    function cleanup() {
      if (ended) return;
      ended = true;
      try { sourceStream.destroy(); } catch (e) {}
      try { parser.destroy(); } catch (e) {}
      try { res.end(); } catch (e) {}
    }

    // NDJSON streaming mode: writes one JSON object per line as rows are parsed.
    if (mode === 'ndjson') {
      res.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
      sourceStream.pipe(parser);

      parser.on('data', (row) => {
        rowIndex++;
        if (rowIndex <= offset) return; // skip until offset

        const line = JSON.stringify(row) + '\n';
        const ok = res.write(line);
        sent++;

        if (limit && sent >= limit) {
          cleanup();
        }
      });

      parser.on('end', () => { cleanup(); });
      parser.on('error', (err) => {
        console.error('CSV parser error:', err);
        if (!ended) {
          res.status(500).json({ error: 'CSV parse error' });
        }
        cleanup();
      });

      sourceStream.on('error', (err) => {
        console.error('Request stream error:', err);
        if (!ended) {
          res.status(500).json({ error: 'Failed to fetch CSV stream' });
        }
        cleanup();
      });

    } else {
      // JSON mode with memory protection and simple pagination (limit/offset)
      const rows = [];
      sourceStream.pipe(parser);

      parser.on('data', (row) => {
        rowIndex++;
        if (rowIndex <= offset) return; // skip until offset

        rows.push(row);
        sent++;

        // If user specified a limit, stop when reached
        if (limit && sent >= limit) {
          parser.pause();
          cleanup();
        }

        // If no limit provided, enforce a server-side cap to avoid OOM / huge responses
        if (!limit && rows.length > MAX_PROTECT) {
          console.warn('Server protection: too many rows to return at once');
          parser.pause();
          cleanup();
        }
      });

      parser.on('end', () => {
        // has_more indicates if there are more rows after the returned slice
        const has_more = (limit && rowIndex > offset + rows.length) || (!limit && rowIndex > rows.length + offset);
        res.json({ rows, offset, returned: rows.length, has_more });
      });

      parser.on('error', (err) => {
        console.error('CSV parser error:', err);
        if (!ended) {
          res.status(500).json({ error: 'CSV parse error' });
        }
        cleanup();
      });

      sourceStream.on('error', (err) => {
        console.error('Request stream error:', err);
        if (!ended) {
          res.status(500).json({ error: 'Failed to fetch CSV stream' });
        }
        cleanup();
      });
    }

  } catch (error) {
    console.error('Error fetching CSV:', error.message);
    res.status(500).json({ error: 'Failed to fetch or parse CSV' });
  }
});


const PORT = process.env.PORT || 3000;
app.listen(PORT, (err) => {
  if (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
  console.log(`MCP server running on port ${PORT}`);
});
