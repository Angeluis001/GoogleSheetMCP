const express = require('express');
const axios = require('axios');
const { parse } = require('csv-parse/sync');
// Add streaming CSV parser for pagination
const { parse: createCsvParser } = require('csv-parse');


const app = express();
app.use(express.json());

// Log every incoming request
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

app.post('/gsheets_export_csv', async (req, res) => {
  const { url } = req.body;

  try {
    const response = await axios.get(url);
    const records = parse(response.data, {
      columns: true,
      skip_empty_lines: true
    });

    res.json({ rows: records });
  } catch (error) {
    console.error('Error fetching CSV:', error.message);
    res.status(500).json({ error: 'Failed to fetch or parse CSV' });
  }
});

// New: paginated, streaming endpoint to limit payload size
app.post('/gsheets_export_csv_page', async (req, res) => {
  const { url, offset = 0, limit = 200, columns } = req.body || {};

  if (!url) {
    return res.status(400).json({ error: 'url is required' });
  }

  const off = Math.max(0, parseInt(offset, 10) || 0);
  const limCap = 1000; // hard cap to protect clients
  const lim = Math.min(limCap, Math.max(1, parseInt(limit, 10) || 200));

  try {
    const response = await axios.get(url, { responseType: 'stream' });

    const parser = createCsvParser({
      columns: true,
      skip_empty_lines: true
    });

    const rows = [];
    let read = 0;
    let hasMore = false;
    let responded = false;

    parser.on('data', (record) => {
      if (responded) return; // safety

      if (read < off) {
        read += 1;
        return;
      }

      if (rows.length < lim) {
        if (Array.isArray(columns) && columns.length) {
          const projected = {};
          for (const c of columns) projected[c] = record[c];
          rows.push(projected);
        } else {
          rows.push(record);
        }
        return;
      }

      // If we get here, there is at least one more row beyond the page
      hasMore = true;
      // Stop consuming more data to save resources
      responded = true;
      parser.destroy();
      if (response.data && typeof response.data.destroy === 'function') {
        response.data.destroy();
      }
    });

    parser.on('error', (err) => {
      if (responded) return;
      responded = true;
      console.error('CSV parse error:', err.message);
      res.status(500).json({ error: 'Failed to parse CSV' });
    });

    parser.on('end', () => {
      if (responded) return;
      responded = true;
      res.json({
        rows,
        page: {
          offset: off,
          limit: lim,
          returned: rows.length,
          hasMore,
          nextOffset: hasMore ? off + lim : null
        }
      });
    });

    response.data.pipe(parser);
  } catch (error) {
    console.error('Error fetching CSV:', error.message);
    res.status(500).json({ error: 'Failed to fetch CSV' });
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
