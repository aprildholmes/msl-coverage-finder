require('dotenv').config();

const express  = require('express');
const session  = require('express-session');
const multer   = require('multer');
const XLSX     = require('xlsx');
const low      = require('lowdb');
const FileSync = require('lowdb/adapters/FileSync');
const path     = require('path');
const fs       = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;
const ADMIN_PIN = process.env.ADMIN_PIN || '1234';

// ── Data directory ──────────────────────────────────────────────────────────
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

// ── lowdb setup ─────────────────────────────────────────────────────────────
// lowdb v1 is pure JavaScript — no compilation required.
// All data is persisted to data/db.json automatically.
const adapter = new FileSync(path.join(dataDir, 'db.json'));
const db      = low(adapter);

db.defaults({
  ztt_entries: [],   // { zip, msl_name, msl_email, territory, team }
  msls:        [],   // { name, email, team }
  coverages:   []    // { id, from_name, from_email, to_name, to_email,
                     //   start_date, end_date, created_by, created_at, expired, team }
}).write();

// ── Middleware ───────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || 'msl-finder-session-secret',
  resave: false,
  saveUninitialized: false,
  cookie: { secure: false, maxAge: 8 * 60 * 60 * 1000 } // 8 hours
}));

// Multer — memory storage (files are processed in-memory, never written to disk)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 } // 20 MB
});

// ── Helpers ──────────────────────────────────────────────────────────────────
function todayStr() {
  return new Date().toISOString().split('T')[0];
}

/** Mark any coverage whose end_date has passed as expired, then persist. */
function archiveExpiredCoverages() {
  const today   = todayStr();
  let   changed = false;

  db.get('coverages').value().forEach(c => {
    if (!c.expired && c.end_date < today) {
      c.expired = true;   // mutate the in-memory object directly
      changed   = true;
    }
  });

  if (changed) db.write();
}

/** Admin-only middleware — returns 401 if session is not authenticated. */
function requireAdmin(req, res, next) {
  if (req.session && req.session.adminAuth) return next();
  res.status(401).json({ error: 'Unauthorized' });
}

/** Generate the next integer ID for a new coverage record. */
function nextCoverageId() {
  const items = db.get('coverages').value();
  if (items.length === 0) return 1;
  return Math.max(...items.map(i => i.id || 0)) + 1;
}

/** Parse an uploaded buffer (XLSX or CSV) into normalised row objects. */
function parseUploadBuffer(buffer, filename) {
  const ext = filename.split('.').pop().toLowerCase();
  let rows  = [];

  if (ext === 'xlsx') {
    const wb  = XLSX.read(buffer, { type: 'buffer' });
    const ws  = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
    const start = String(raw[0][0]).toLowerCase().includes('zip') ? 1 : 0;
    rows = raw.slice(start)
      .filter(r => r[0])
      .map(r => ({
        zip:       String(r[0]).trim(),
        territory: String(r[1] || '').trim(),
        msl_name:  String(r[2] || '').trim(),
        msl_email: String(r[3] || '').trim()
      }));
  } else {
    // CSV / TSV
    const text  = buffer.toString('utf8');
    const lines = text.split('\n').filter(l => l.trim());
    const start = lines[0].toLowerCase().includes('zip') ? 1 : 0;
    const delim = lines[0].includes('\t') ? '\t' : ',';

    for (let i = start; i < lines.length; i++) {
      const cols = lines[i].split(delim).map(c => c.trim().replace(/^"|"$/g, ''));
      if (cols.length >= 4) {
        rows.push({ zip: cols[0], territory: cols[1], msl_name: cols[2], msl_email: cols[3] });
      } else if (cols.length === 3) {
        rows.push({ zip: cols[0], territory: '',      msl_name: cols[1], msl_email: cols[2] });
      }
    }
  }

  // Only keep rows that have a zip and a full MSL name (must contain a space)
  return rows.filter(r => r.zip && r.msl_name && r.msl_name.includes(' '));
}

/** Returns sorted list of distinct team names present in ztt_entries. */
function getTeams() {
  const seen = new Set();
  db.get('ztt_entries').value().forEach(e => seen.add(e.team || 'Default'));
  return [...seen].sort();
}

/**
 * Replace ztt_entries and msls for the given team only.
 * Data belonging to other teams is preserved.
 */
function saveRows(rows, teamName) {
  const otherEntries = db.get('ztt_entries').value().filter(e => (e.team || 'Default') !== teamName);
  const otherMsls    = db.get('msls').value().filter(m => (m.team || 'Default') !== teamName);

  const newEntries = [];
  const mslsSeen   = {};
  const newMsls    = [];

  rows.forEach(r => {
    newEntries.push({ zip: r.zip, msl_name: r.msl_name, msl_email: r.msl_email, territory: r.territory, team: teamName });
    if (!mslsSeen[r.msl_name]) {
      mslsSeen[r.msl_name] = true;
      newMsls.push({ name: r.msl_name, email: r.msl_email, team: teamName });
    }
  });

  db.set('ztt_entries', [...otherEntries, ...newEntries])
    .set('msls', [...otherMsls, ...newMsls])
    .write();
}

// ── API Routes ───────────────────────────────────────────────────────────────

// ---- Admin auth ----

app.post('/api/admin/verify', (req, res) => {
  const { pin } = req.body;
  if (String(pin) === String(ADMIN_PIN)) {
    req.session.adminAuth = true;
    res.json({ success: true });
  } else {
    res.status(401).json({ success: false, error: 'Incorrect PIN' });
  }
});

app.get('/api/admin/status', (req, res) => {
  res.json({ authenticated: !!(req.session && req.session.adminAuth) });
});

app.post('/api/admin/logout', (req, res) => {
  if (req.session) req.session.adminAuth = false;
  res.json({ success: true });
});

// ---- Initial data load ----

app.get('/api/data', (req, res) => {
  archiveExpiredCoverages();

  const teams    = getTeams();
  const teamData = {};

  teams.forEach(team => {
    const entries = db.get('ztt_entries').value().filter(e => (e.team || 'Default') === team);
    const mslList = db.get('msls').value()
      .filter(m => (m.team || 'Default') === team)
      .slice()
      .sort((a, b) => a.name.localeCompare(b.name));

    const zttData = {};
    entries.forEach(e => {
      zttData[e.zip] = { msl_name: e.msl_name, msl_email: e.msl_email, territory: e.territory };
    });

    teamData[team] = { zttData, mslList };
  });

  const coverages = db.get('coverages').value()
    .filter(c => !c.expired)
    .sort((a, b) => a.start_date.localeCompare(b.start_date));
  const history = db.get('coverages').value()
    .filter(c => c.expired)
    .sort((a, b) => b.end_date.localeCompare(a.end_date));

  res.json({ teams, teamData, coverages, history });
});

// ---- Zip code search ----

app.get('/api/search', (req, res) => {
  archiveExpiredCoverages();

  const { zip, team } = req.query;
  if (!zip || zip.length !== 5) {
    return res.status(400).json({ error: 'Invalid zip code' });
  }
  if (!team) {
    return res.status(400).json({ error: 'Team is required' });
  }

  const entry = db.get('ztt_entries').value()
    .find(e => e.zip === zip && (e.team || 'Default') === team);
  if (!entry) return res.json({ found: false });

  const today    = todayStr();
  const coverage = db.get('coverages').value().find(
    c => c.from_name === entry.msl_name &&
         (c.team || 'Default') === team  &&
         !c.expired                      &&
         c.start_date <= today           &&
         c.end_date   >= today
  );

  res.json({ found: true, entry, coverage: coverage || null });
});

// ---- File upload ----

app.post('/api/upload', requireAdmin, upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  const teamName = (req.body.team || '').trim();
  if (!teamName) return res.status(400).json({ error: 'Team name is required.' });

  try {
    const rows = parseUploadBuffer(req.file.buffer, req.file.originalname);
    if (rows.length === 0) {
      return res.status(400).json({
        error: 'No valid rows found. Ensure the file has columns: zip, territory_name, msl_name, msl_email'
      });
    }
    saveRows(rows, teamName);
    const mslCount = db.get('msls').value().filter(m => (m.team || 'Default') === teamName).length;
    res.json({ success: true, zipCount: rows.length, mslCount, teamName, preview: rows.slice(0, 10) });
  } catch (err) {
    res.status(500).json({ error: 'Failed to process file: ' + err.message });
  }
});

// ---- Sample data ----

app.post('/api/sample', requireAdmin, (req, res) => {
  const teamName = ((req.body && req.body.team) || 'Sample').trim() || 'Sample';
  const rows = [
    { zip: '10001', msl_name: 'Sarah Mitchell', msl_email: 's.mitchell@pharma.com', territory: '' },
    { zip: '10002', msl_name: 'Sarah Mitchell', msl_email: 's.mitchell@pharma.com', territory: '' },
    { zip: '10003', msl_name: 'Sarah Mitchell', msl_email: 's.mitchell@pharma.com', territory: '' },
    { zip: '30301', msl_name: 'James Okafor',   msl_email: 'j.okafor@pharma.com',  territory: '' },
    { zip: '30302', msl_name: 'James Okafor',   msl_email: 'j.okafor@pharma.com',  territory: '' },
    { zip: '30303', msl_name: 'James Okafor',   msl_email: 'j.okafor@pharma.com',  territory: '' },
    { zip: '77001', msl_name: 'Priya Nair',     msl_email: 'p.nair@pharma.com',    territory: '' },
    { zip: '77002', msl_name: 'Priya Nair',     msl_email: 'p.nair@pharma.com',    territory: '' },
    { zip: '60601', msl_name: 'Dana Reyes',     msl_email: 'd.reyes@pharma.com',   territory: '' },
    { zip: '60602', msl_name: 'Dana Reyes',     msl_email: 'd.reyes@pharma.com',   territory: '' },
    { zip: '90001', msl_name: 'Tom Welling',    msl_email: 't.welling@pharma.com', territory: '' },
    { zip: '90002', msl_name: 'Tom Welling',    msl_email: 't.welling@pharma.com', territory: '' }
  ];

  saveRows(rows, teamName);
  const mslCount = db.get('msls').value().filter(m => (m.team || 'Default') === teamName).length;
  res.json({ success: true, zipCount: rows.length, mslCount, teamName, preview: rows.slice(0, 10) });
});

// ---- Coverage management (admin-only) ----

app.get('/api/coverages', requireAdmin, (req, res) => {
  archiveExpiredCoverages();
  const coverages = db.get('coverages').value()
    .filter(c => !c.expired)
    .sort((a, b) => a.start_date.localeCompare(b.start_date));
  res.json(coverages);
});

app.post('/api/coverage', requireAdmin, (req, res) => {
  const { from_name, to_name, start_date, end_date, team } = req.body;

  if (!from_name || !to_name || !start_date || !end_date) {
    return res.status(400).json({ error: 'All fields are required.' });
  }
  if (from_name === to_name) {
    return res.status(400).json({ error: 'The covering MSL must be different from the MSL on leave.' });
  }
  if (start_date > end_date) {
    return res.status(400).json({ error: 'End date must be after start date.' });
  }

  const teamName = (team || '').trim() || 'Default';
  const fromMsl  = db.get('msls').value().find(m => m.name === from_name && (m.team || 'Default') === teamName);
  const toMsl    = db.get('msls').value().find(m => m.name === to_name   && (m.team || 'Default') === teamName);
  if (!fromMsl || !toMsl) {
    return res.status(400).json({ error: 'Invalid MSL selection.' });
  }

  const now = new Date();
  const createdAt =
    now.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' }) +
    ' ' +
    now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

  const newCoverage = {
    id:         nextCoverageId(),
    from_name:  fromMsl.name,
    from_email: fromMsl.email,
    to_name:    toMsl.name,
    to_email:   toMsl.email,
    start_date,
    end_date,
    created_by: 'Admin',
    created_at: createdAt,
    expired:    false,
    team:       teamName
  };

  db.get('coverages').push(newCoverage).write();
  res.json({ success: true, id: newCoverage.id });
});

app.delete('/api/coverage/:id', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  db.get('coverages').remove(c => c.id === id).write();
  res.json({ success: true });
});

// ---- Remove a single team (all its ZTT data + coverages) ----

app.delete('/api/team/:name', (req, res) => {
  const teamName = decodeURIComponent(req.params.name);
  db.get('ztt_entries').remove(e => (e.team || 'Default') === teamName).write();
  db.get('msls').remove(m => (m.team || 'Default') === teamName).write();
  db.get('coverages').remove(c => (c.team || 'Default') === teamName).write();
  res.json({ success: true, teams: getTeams() });
});

// ---- Coverage history (admin-only) ----

app.get('/api/history', requireAdmin, (req, res) => {
  archiveExpiredCoverages();
  const history = db.get('coverages').value()
    .filter(c => c.expired)
    .sort((a, b) => b.end_date.localeCompare(a.end_date));
  res.json(history);
});

// ---- Clear all data (admin-only) ----

app.post('/api/clear', requireAdmin, (req, res) => {
  db.set('ztt_entries', [])
    .set('msls',        [])
    .set('coverages',   [])
    .write();
  if (req.session) req.session.adminAuth = false;
  res.json({ success: true });
});

// ── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n  MSL Coverage Finder is running at http://localhost:${PORT}\n`);
});
