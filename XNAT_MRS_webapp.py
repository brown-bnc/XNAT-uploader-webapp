#XNAT_MRS_webapp.py — Flask web‑app for bulk uploading .rda & .dat spectroscopy resources to XNAT
# ====================================================================
"""
Quick start
~~~~~~~~~~
```bash
python -m pip install Flask          # if not installed
export XNAT_BASE_URL=https://xnat.bnc.brown.edu  # or any XNAT
python XNATwebapp_twix.py
# → open http://127.0.0.1:5000, log in, upload files
```
"""
from __future__ import annotations

import argparse
import base64
import os
import re
import sys
import unittest
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta
from typing import Dict, List, Optional, Tuple, Any
import json
import time
from socket import timeout as SocketTimeout
from dataclasses import dataclass
from typing import Optional
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import secrets
import shutil

# ---------------------------------------------------------------------------
# Graceful Flask import
# ---------------------------------------------------------------------------
try:
    from flask import (
        Flask,
        flash,
        redirect,
        render_template_string,
        request,
        session,
        url_for,
        Response,
        abort,
    )
except ModuleNotFoundError:
    print("ERROR: Flask not installed →  python -m pip install Flask")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
XNAT_BASE_URL: str = os.getenv("XNAT_BASE_URL", "https://xnat.bnc.brown.edu").rstrip("/")
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-pls")
app.permanent_session_lifetime = timedelta(minutes=5)

# defaults chosen for large files over VPN/Wi-Fi
XNAT_HTTP_TIMEOUT = int(os.getenv("XNAT_HTTP_TIMEOUT", "300"))   # seconds
XNAT_HTTP_RETRIES = int(os.getenv("XNAT_HTTP_RETRIES", "3"))     # attempts

# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------

LOGIN_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Login to XNAT</title>
<style>
  body {
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    background: #f7f9fb;
    display: flex;
    justify-content: center;
    align-items: center;
    height: 100vh;
    margin: 0;
  }

  .login-container {
    background: white;
    padding: 2.5rem 3rem;
    border-radius: 8px;
    box-shadow: 0 4px 18px rgba(0,0,0,0.1);
    width: 100%;
    max-width: 400px;
  }

  h2 {
    text-align: center;
    margin-bottom: 1.5rem;
    color: #222;
  }

  label {
    display: block;
    margin: 0.75rem 0 0.3rem;
    font-weight: 600;
    color: #333;
  }

  input[type=text], input[type=password] {
    width: 100%;
    padding: 0.6rem 0.8rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 1rem;
    box-sizing: border-box;
  }

  input[type=text]:focus, input[type=password]:focus {
    border-color: #4285f4;
    outline: none;
    box-shadow: 0 0 0 2px rgba(66,133,244,0.2);
  }

  button {
    width: 100%;
    margin-top: 1.2rem;
    padding: 0.7rem;
    background: #4285f4;
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 1rem;
    cursor: pointer;
    font-weight: 600;
  }

  button:hover {
    background: #357ae8;
  }

  .flash {
    background: #fdecea;
    border: 1px solid #f5c2c0;
    color: #b12a2a;
    padding: 0.8rem;
    border-radius: 4px;
    margin-bottom: 1rem;
    text-align: center;
  }

  footer {
    text-align: center;
    font-size: 0.85rem;
    margin-top: 1.5rem;
    color: #777;
  }
</style>
</head>
<body>
  <div class="login-container">
    <h2>XNAT Login</h2>

    {% with m=get_flashed_messages() %}
      {% if m %}
        {% for msg in m %}
          <div class="flash">{{ msg }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}

    <form method="post">
      <label for="username">Username</label>
      <input id="username" name="username" type="text" required>

      <label for="password">Password</label>
      <input id="password" name="password" type="password" required>

      <button type="submit">Log In</button>
    </form>

    <footer>Brown University • Behavior & Neurodata Core</footer>
  </div>
</body>
</html>
"""

UPLOAD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Raw Spectroscopy Data XNAT Uploader</title>
<style>
  :root{
    --bg:#f7f9fb; --card:#fff; --text:#222; --muted:#777; --border:#ddd;
    --primary:#4285f4; --primary-hover:#357ae8;
    --flash-bg:#eef3ff; --flash-border:#dfe8ff; --flash-text:#4285f4;
  }
  html, body { height: 100%; }
  body {
    margin: 0; background: var(--bg);
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    color: var(--text);
    display:flex; justify-content:center; align-items:flex-start;
  }
  .container { width: 100%; max-width: 2000px; margin: 3rem 1rem; }
  .card { background: var(--card); border-radius:8px; box-shadow:0 4px 18px rgba(0,0,0,.08); padding:1.25rem 1.5rem 1.5rem; }
  .card-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:.75rem; }
  h2 { margin:0; font-size:1.25rem; letter-spacing:.2px; }
  .toolbar a { text-decoration:none; color:var(--primary); font-weight:600; }
  .toolbar a:hover { color:var(--primary-hover); }
  .flash { background:var(--flash-bg); border:1px solid var(--flash-border); color:var(--flash-text); padding:.75rem .9rem; border-radius:6px; margin:.75rem 0 1rem; }
  label { display:block; font-weight:600; margin:.6rem 0 .35rem; }
  input[type=file], input[type=text] {
    width:100%; padding:.6rem .75rem; border:1px solid var(--border); border-radius:6px; box-sizing:border-box; font-size:.98rem; background:#fff;
  }
  input[type=text]:focus, input[type=file]:focus { border-color:var(--primary); outline:none; box-shadow:0 0 0 2px rgba(66,133,244,.18); }
  .actions { display:flex; gap:.6rem; margin-top:1rem; flex-wrap:wrap; }
  button, .btn { appearance:none; border:none; border-radius:6px; padding:.65rem 1rem; font-weight:700; cursor:pointer; font-size:.98rem; }
  .btn-primary { background:var(--primary); color:#fff; } .btn-primary:hover{ background:var(--primary-hover); }
  .btn-ghost { background:#eef3ff; color:var(--primary); border:1px solid #dfe8ff; } .btn-ghost:hover{ background:#e3ecff; border-color:#d2e0ff; }
  .table-wrap { margin-top:1rem; border:1px solid var(--border); border-radius:8px; overflow:hidden; background:#fff; }
  table.file-table { width:100%; border-collapse:collapse; table-layout:fixed; }
  table.file-table colgroup col:nth-child(1){width:38%;}
  table.file-table colgroup col:nth-child(2){width:5%;}
  table.file-table colgroup col:nth-child(3){width:24%;}
  table.file-table colgroup col:nth-child(4){width:11%;}
  table.file-table colgroup col:nth-child(5){width:11%;}
  table.file-table colgroup col:nth-child(6){width:11%;}
  table.file-table colgroup col:nth-child(7){width:5%;}
  thead th { text-align:left; background:#f5f7fb; color:#333; padding:.6rem .6rem; border-bottom:1px solid var(--border); font-weight:700; font-size:.8rem; }
  tbody td { border-top:1px solid var(--border); padding:.5rem .5rem; vertical-align:middle; background:#fff; }
  tbody td input[type=text]{ width:100%; padding:.45rem .55rem; border:1px solid #e6e6e6; border-radius:5px; font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; font-size:.8rem; }
  tbody td input[type=text]:focus{ border-color:var(--primary); box-shadow:0 0 0 2px rgba(66,133,244,.15); outline:none; }
  tbody td input[readonly]{ background:#fafafa; color:#444; }
  .remove-btn{ display:inline-flex; align-items:center; justify-content:center; width:2rem; height:2rem; border:none; background:transparent; font-size:1.1rem; color:#a00; cursor:pointer; }
  .remove-btn:hover{ color:#d00; }
  #spinner-overlay { display:none; position:fixed; inset:0; background:rgba(255,255,255,.9); z-index:9999; justify-content:center; align-items:center; gap:.8rem; color:#333; font-weight:600; }
  .spinner { border:6px solid #ccd6ff; border-top:6px solid var(--primary); border-radius:50%; width:2.4rem; height:2.4rem; animation:spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .subtle { color:var(--muted); font-size:.9rem; }
</style>
</head>
<body>
  <div class="container">
    <div class="card">
      <div class="card-header">
        <h2>Raw Spectroscopy Data XNAT Uploader</h2>
        <div class="toolbar"><a href="{{ url_for('logout') }}">Logout</a></div>
      </div>

      {% with messages=get_flashed_messages() %}
        {% for msg in messages %}<div class="flash">{{ msg}}</div>{% endfor %}
      {% endwith %}

      <form id="uploadForm" method="post" action="{{ url_for('upload') }}" enctype="multipart/form-data" novalidate>
        <label for="rda_input">Select .rda and .dat file(s)</label>
        <input id="rda_input" name="files" type="file" accept=".rda,.dat" multiple>

        <div class="subtle" style="margin-top:.35rem;">
          TIP: RDA file headers are used to automatically determine Project, Subject, Session, & Scan. TWIX (.dat) files inherit this information
          from their matching RDA file. If you manually edit any information for an RDA file, the edits will also apply to the matching .dat file, unless you separately change that row.
        </div>

        <div class="table-wrap">
          <table class="file-table">
            <colgroup><col><col><col><col><col><col><col></colgroup>
            <thead>
              <tr>
                <th>Filename</th>
                <th>Scan&nbsp;ID</th>
                <th>Series&nbsp;Description</th>
                <th>Project</th>
                <th>Subject</th>
                <th>Session</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="fileTableBody"></tbody>
          </table>
        </div>

        <div class="actions">
          <button type="button" id="preview_xnat_btn" class="btn btn-ghost">👁 Preview XNAT Session(s)</button>
          <button type="submit" class="btn btn-primary">Upload</button>
        </div>
      </form>
    </div>
  </div>

  <div id="spinner-overlay"><div class="spinner"></div>Uploading…</div>

<script>
/* ===== Debug helpers: surface any runtime error ===== */
window.addEventListener('error', e => console.error('JS Error:', e.message, e.error));
function log(){ try{ console.log.apply(console, arguments); }catch(_){} }
function warn(){ try{ console.warn.apply(console, arguments); }catch(_){} }
function err(){ try{ console.error.apply(console, arguments); }catch(_){} }

/* ===== State maps ===== */
let previewWins = [];
const rdaRowByKey = new Map();
const datRowsByKey = new Map();

/* ===== Utils ===== */
function sanitize(txt){ return String(txt||'').trim().replace(/\\W+/g,'_').replace(/^_|_$/g,''); }
function normSeriesKey(txt){ return String(txt||'').toLowerCase().replace(/[_-]/g,''); }
function parseDatSeries(name){
  const m = String(name||'').match(/^meas_[^_]+_[^_]+_(.+)\\.dat$/i);
  return m ? m[1] : '';
}
function readFileText(file){
  return new Promise((resolve,reject)=>{
    const r = new FileReader();
    r.onload = e => resolve(String(e.target && e.target.result || ''));
    r.onerror = e => reject(e);
    try {
      // latin-1 improves robustness with Siemens headers
      r.readAsText(file, 'ISO-8859-1');
    } catch (_) {
      r.readAsText(file); // fallback
    }
  });
}
function parseRDA(text){
  try{
    const start='>>> Begin of header <<<', end='>>> End of header <<<';
    const a = text.indexOf(start), b = text.indexOf(end);
    if (a === -1 || b === -1) return {};
    const lines = text.slice(a + start.length, b).split(/\\r?\\n/);
    const dict = {};
    for (var i=0;i<lines.length;i++){
      const ln = lines[i]; const j = ln.indexOf(':');
      if (j > 0){
        const k = ln.slice(0,j).trim();
        const v = ln.slice(j+1).trim();
        dict[k] = v;
      }
    }
    return dict;
  }catch(e){ err('parseRDA failed:', e); return {}; }
}

/* ===== Table helpers ===== */
function rowVals(row){
  return {
    project: (row.querySelector("input[name='project_ids']")||{}).value ? row.querySelector("input[name='project_ids']").value.trim() : '',
    subject: (row.querySelector("input[name='subject_labels']")||{}).value ? row.querySelector("input[name='subject_labels']").value.trim() : '',
    session: (row.querySelector("input[name='experiment_labels']")||{}).value ? row.querySelector("input[name='experiment_labels']").value.trim() : '',
    scan:    (row.querySelector("input[name='scan_ids']")||{}).value ? row.querySelector("input[name='scan_ids']").value.trim() : ''
  };
}
function propagateFromRda(key){
  const src = rdaRowByKey.get(key);
  if (!src) return;
  const vals = rowVals(src);
  const targets = datRowsByKey.get(key) || [];
  for (var t=0; t<targets.length; t++){
    const row = targets[t];
    const fields = {
      project_ids: vals.project,
      subject_labels: vals.subject,
      experiment_labels: vals.session,
      scan_ids: vals.scan
    };
    for (var name in fields){
      if (!Object.prototype.hasOwnProperty.call(fields, name)) continue;
      const inp = row.querySelector("input[name='" + name + "']");
      if (!inp) continue;
      if (inp.dataset.dirty === "1") continue;
      inp.value = fields[name]; inp.title = fields[name];
    }
  }
}
function addRow(kind, fileName, meta){
  const tbody = document.getElementById('fileTableBody');
  if (!tbody){ err('tbody not found'); return; }
  const row = document.createElement('tr');
  row.dataset.kind = kind;
  row.dataset.seriesKey = meta.key || '';
  row.dataset.filename = fileName;

  // hidden token: used to re-load staged bytes on retry (server-side)
  const tok = document.createElement('input');
  tok.type = 'hidden';
  tok.name = 'file_tokens';
  tok.value = meta.token || '';
  row.appendChild(tok);

  function mkCell(name, val, readonly){
    const td = document.createElement('td');
    const inp = document.createElement('input');
    inp.type = 'text'; inp.name = name;
    inp.value = val || ''; inp.title = val || '';
    if (readonly) inp.readOnly = true;
    if (kind === 'dat' && !readonly){
      inp.addEventListener('input', function(){ inp.dataset.dirty = '1'; });
    }
    td.appendChild(inp);
    return td;
  }

  row.appendChild(mkCell('file_names', fileName, true));
  row.appendChild(mkCell('scan_ids', meta.scan ? String(meta.scan) : ''));
  row.appendChild(mkCell('series_descs', meta.series ? String(meta.series) : ''));
  row.appendChild(mkCell('project_ids', meta.project ? String(meta.project) : ''));
  row.appendChild(mkCell('subject_labels', meta.subject ? String(meta.subject) : ''));
  row.appendChild(mkCell('experiment_labels', meta.session ? String(meta.session) : ''));

  const tdRemove = document.createElement('td');
  const btn = document.createElement('button');
  btn.type = 'button'; btn.className = 'remove-btn'; btn.textContent = '❌';
  btn.addEventListener('click', function(){
    const key = row.dataset.seriesKey;
    if (row.dataset.kind === 'rda'){ rdaRowByKey.delete(key); }
    if (row.dataset.kind === 'dat'){
      const arr = datRowsByKey.get(key) || [];
      datRowsByKey.set(key, arr.filter(function(r){ return r !== row; }));
    }
    row.remove();
  });
  tdRemove.appendChild(btn);
  row.appendChild(tdRemove);

  tbody.appendChild(row);

  const key = row.dataset.seriesKey;
  if (kind === 'rda'){
    rdaRowByKey.set(key, row);
    ['project_ids','subject_labels','experiment_labels','scan_ids'].forEach(function(sel){
      row.querySelector("input[name='" + sel + "']").addEventListener('input', function(){ propagateFromRda(key); });
    });
    propagateFromRda(key);
  } else {
    const arr = datRowsByKey.get(key) || [];
    arr.push(row); datRowsByKey.set(key, arr);
  }
}

/* ===== File handling ===== */
async function handleFiles(fileList){
  try{
    const files = Array.prototype.slice.call(fileList || []);
    log('Selected files:', files.map(f => f.name));

    const tbody = document.getElementById('fileTableBody');
    if (!tbody){ err('No tbody'); return; }
    tbody.innerHTML = '';
    rdaRowByKey.clear(); datRowsByKey.clear();

    // RDAs first
    const rdaFiles = files.filter(function(f){ return /\\.rda$/i.test(f.name); });
    const rdaInfos = [];
    for (var i=0; i<rdaFiles.length; i++){
      const f = rdaFiles[i];
      let txt = '';
      try { txt = await readFileText(f); }
      catch(ex){ warn('FileReader failed for', f.name, ex); continue; }

      const hdr = parseRDA(txt);
      const meta = {
        scan: (hdr.SeriesNumber || '').trim ? (hdr.SeriesNumber || '').trim() : (hdr.SeriesNumber || ''),
        series: (hdr.SeriesDescription || '').trim ? (hdr.SeriesDescription || '').trim() : (hdr.SeriesDescription || ''),
        project: sanitize(hdr.StudyDescription || ''),
        subject: sanitize(hdr.PatientName || ''),
        session: sanitize(hdr.PatientID || ''),
        key: normSeriesKey(hdr.SeriesDescription || '')
      };
      rdaInfos.push({ file:f, meta:meta });
    }
    for (var j=0; j<rdaInfos.length; j++){ addRow('rda', rdaInfos[j].file.name, rdaInfos[j].meta); }

    const rdaMetaByKey = new Map(
      rdaInfos.filter(function(x){ return !!x.meta.key; }).map(function(x){ return [x.meta.key, x.meta]; })
    );

    // DATs next (still add rows even if no matching RDA)
    const datFiles = files.filter(function(f){ return /\\.dat$/i.test(f.name); });
    for (var k=0; k<datFiles.length; k++){
      const f = datFiles[k];
      const seriesRaw = parseDatSeries(f.name);
      const key = normSeriesKey(seriesRaw);
      const rdaMeta = rdaMetaByKey.get(key) || { scan:'', series:seriesRaw, project:'', subject:'', session:'', key:key };
      const meta = {
        scan: rdaMeta.scan,
        series: rdaMeta.series || seriesRaw,
        project: rdaMeta.project,
        subject: rdaMeta.subject,
        session: rdaMeta.session,
        key: key
      };
      addRow('dat', f.name, meta);
    }

    log('Table rows now:', document.querySelectorAll('#fileTableBody tr').length);
  } catch (e){
    err('handleFiles failed:', e);
    alert('Problem processing files. See console for details.');
  }
}

/* ===== Preview ===== */
function openXNATPreviewsFromTable() {
  const rows = document.querySelectorAll('#fileTableBody tr');
  const uniq = new Set();
  for (var i=0;i<rows.length;i++){
    const row = rows[i];
    const p = (row.querySelector("input[name='project_ids']")||{}).value || '';
    const s = (row.querySelector("input[name='subject_labels']")||{}).value || '';
    const e = (row.querySelector("input[name='experiment_labels']")||{}).value || '';
    if (p && s && e) uniq.add(p + '|||' + s + '|||' + e);
  }
  const xnatBase = {{ XNAT_BASE_URL | tojson }};
  previewWins = [];
  uniq.forEach(function(key){
    const parts = key.split('|||'); const p = parts[0], s = parts[1], e = parts[2];
    const url = xnatBase.replace(/\\/$/,'') + '/data/projects/' +
      encodeURIComponent(p) + '/subjects/' + encodeURIComponent(s) +
      '/experiments/' + encodeURIComponent(e);
    const w = window.open(url, '_blank'); if (w) previewWins.push(w);
  });
}

/* ===== DOM ready ===== */
window.addEventListener('DOMContentLoaded', function(){
  const fileInput = document.getElementById('rda_input');
  const form = document.getElementById('uploadForm');
  if (!fileInput){ err('#rda_input missing'); return; }

  {% if pending_rows %}
  try {
    const rows = {{ pending_rows | tojson }};
    const tbody = document.getElementById('fileTableBody');
    tbody.innerHTML = '';
    rdaRowByKey.clear(); datRowsByKey.clear();

    rows.forEach(r => {
      addRow(r.kind, r.filename, {
        scan: r.scan_id || '',
        series: r.series_desc || '',
        project: r.project || '',
        subject: r.subject || '',
        session: r.session || '',
        key: r.key || '',
        token: r.token || ''
      });
    });
  } catch (e) {
    console.warn("Failed to restore pending rows", e);
  }
  {% endif %}

  fileInput.addEventListener('change', function(e){ handleFiles(e.target.files); });
  const prevBtn = document.getElementById('preview_xnat_btn');
  if (prevBtn) prevBtn.addEventListener('click', openXNATPreviewsFromTable);

  if (form){
    form.addEventListener('submit', function(){
      if (previewWins && previewWins.length){
        previewWins.forEach(function(w){ try{ if (w && !w.closed) w.close(); }catch(_){} });
      }
      document.getElementById('spinner-overlay').style.display = 'flex';
    });
  }

  {% if reload_urls %}
  try {
    const urls = {{ reload_urls | tojson }};
    for (var i=0;i<urls.length;i++){
      const win = window.open(urls[i], '_blank'); if (win) win.focus();
    }
  } catch (e) { warn('XNAT reload failed:', e); }
  {% endif %}
});
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Backend helpers
# ---------------------------------------------------------------------------

def _basic_auth_header(user: str, pwd: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _session_creds() -> Tuple[str, str]:
    if "xnat_user" not in session or "xnat_pass" not in session:
        raise RuntimeError("Not logged in")
    return session["xnat_user"], session["xnat_pass"]

def _check_credentials(user: str, pwd: str) -> bool:
    url = f"{XNAT_BASE_URL}/data/projects?limit=1"
    result = _request("GET", url, None, auth=(user, pwd), timeout=15)
    return bool(result.ok and result.status is not None and 200 <= result.status < 300)


def _init_logging() -> None:
    logdir = Path.home() / ".xnat_uploader"
    logdir.mkdir(exist_ok=True)

    logfile = logdir / "uploader.log"

    handler = RotatingFileHandler(
        logfile,
        maxBytes=2_000_000,   # ~2 MB
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)

_init_logging()

def _stage_dir() -> Path:
    d = Path.home() / ".xnat_uploader" / "staged"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _stage_save_filestorage(fs) -> tuple[str, str]:
    """
    Save an incoming Werkzeug FileStorage to disk.
    Returns (token, path).
    """
    token = secrets.token_urlsafe(16)
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", (fs.filename or "upload"))
    path = _stage_dir() / f"{token}__{safe_name}"
    with open(path, "wb") as out:
        try:
            fs.stream.seek(0)
        except Exception:
            pass
        shutil.copyfileobj(fs.stream, out)
    return token, str(path)

def _staged_info(token: str) -> dict:
    info = session.get("pending_files", {}).get(token)
    if not info:
        raise UserFacingError("Staged file data expired. Please re-select your files and try again.")
    return info

def _staged_open(token: str):
    info = _staged_info(token)
    path = info.get("path")
    if not path or not os.path.exists(path):
        raise UserFacingError("Staged file is missing. Please re-select your files and try again.")
    return open(path, "rb")

def _staged_delete(token: str) -> None:
    pending = session.get("pending_files", {})
    info = pending.get(token)
    if not info:
        return
    path = info.get("path")
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
    pending.pop(token, None)
    session["pending_files"] = pending

def _clear_all_staged_for_session() -> None:
    """
    Delete all staged files referenced by this session and clear session keys.
    Safe to call multiple times.
    """
    pending = session.get("pending_files", {}) or {}
    for tok, info in list(pending.items()):
        path = info.get("path")
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        pending.pop(tok, None)

    session.pop("pending_files", None)
    session.pop("pending_rows", None)


@dataclass
class RequestResult:
    ok: bool
    status: Optional[int] = None
    error_type: Optional[str] = None   # "network", "timeout", "http", "auth"
    message: Optional[str] = None

class UserFacingError(RuntimeError):
    """Error with a message safe to show to end users."""
    pass


def _request(method: str, url: str, data: bytes | None,
             auth: Tuple[str, str] | None = None,
             timeout: int | None = None) -> RequestResult:
    """
    Perform an HTTP request with retries.

    Returns a RequestResult with enough detail to present
    a user-friendly error message.
    """
    user, pwd = auth if auth else _session_creds()
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": _basic_auth_header(user, pwd),
            "Accept": "application/json",
        },
    )

    tries = max(1, XNAT_HTTP_RETRIES)
    to = timeout or XNAT_HTTP_TIMEOUT
    backoff = 1.5

    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=to) as resp:  # type: ignore[arg-type]
                return RequestResult(ok=True, status=resp.status)

        except urllib.error.HTTPError as err:
            # HTTPError is a valid HTTP response (4xx/5xx)
            if err.code in (401, 403):
                return RequestResult(
                    ok=False,
                    status=err.code,
                    error_type="auth",
                    message="Authentication with XNAT failed. Please log in again."
                )

            return RequestResult(
                ok=False,
                status=err.code,
                error_type="http",
                message=f"XNAT returned HTTP {err.code}."
            )

        except SocketTimeout:
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
                continue

            return RequestResult(
                ok=False,
                error_type="timeout",
                message="Network timeout while contacting XNAT."
            )

        except urllib.error.URLError:
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
                continue

            return RequestResult(
                ok=False,
                error_type="network",
                message="Network error while contacting XNAT."
            )

    # Should not be reachable, but keep a safe fallback
    return RequestResult(
        ok=False,
        error_type="unknown",
        message="Unexpected error while contacting XNAT."
    )

def _ensure_resource(base: str, label: str) -> None:
    _request("PUT", f"{base}/resources/{label}", data=b"")

# ---------------------------------------------------------------------------
# helper that returns the decoded JSON body
# ---------------------------------------------------------------------------
def _request_json(url: str,
                  auth: Tuple[str, str] | None = None,
                  timeout: int = 15) -> Any:
    """GET `url` and return the parsed JSON response."""
    user, pwd = auth if auth else _session_creds()
    req = urllib.request.Request(
        url,
        headers={"Authorization": _basic_auth_header(user, pwd),
                 "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)          


# ---------------------------------------------------------------------------
# Convenience wrapper for the dicomdump call
# ---------------------------------------------------------------------------

def dicom_field(base: str, scan_src: str, field: str,
                auth: Tuple[str, str] | None = None) -> str:
    query = urllib.parse.urlencode({"src": scan_src, "field": field})
    url = f"{base}/data/services/dicomdump?{query}"

    try:
        result = _request_json(url, auth=auth)

    except SocketTimeout as err:
        raise UserFacingError(
            "XNAT did not respond in time. Please try again."
        ) from err

    except urllib.error.HTTPError as err:
        if err.code in (401, 403):
            raise UserFacingError(
                "XNAT authentication failed or you do not have access to this project/scan. "
                "Please log in again or verify permissions."
            ) from err
        if err.code == 404:
            raise UserFacingError(
                "XNAT could not find the specified project/subject/session/scan. "
                "Please check the values in the table and try again."
            ) from err
        raise UserFacingError(
            "XNAT returned an error while reading scan metadata. Please try again."
        ) from err

    except urllib.error.URLError as err:
        # DNS failure, connection refused, TLS handshake, etc.
        raise UserFacingError(
            "Unable to reach XNAT. Please check your network connection and try again."
        ) from err

    except json.JSONDecodeError as err:
        raise UserFacingError(
            "XNAT returned an unexpected response while reading scan metadata. Please try again."
        ) from err

    # Payload interpretation (not network errors)
    rows = result.get("ResultSet", {}).get("Result", [])
    if not rows:
        raise UserFacingError(
            "XNAT could not find DICOM metadata for the selected scan. "
            "Please check the project/subject/session/scan values and try again."
        )

    value = rows[0].get("value")
    if value in (None, ""):
        raise UserFacingError(
            "XNAT returned incomplete DICOM metadata for the selected scan."
        )

    return value


# ---------------------------------------------------------------------------
# RDA helpers
# ---------------------------------------------------------------------------

def _parse_hdr(raw: bytes) -> Dict[str, str]:
    s = raw.find(b">>> Begin of header <<<")
    e = raw.find(b">>> End of header <<<")
    if s == -1 or e == -1:
        return {}
    txt = raw[s+24:e].decode('latin-1', errors='replace')
    out: Dict[str, str] = {}
    for line in txt.splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            out[k.strip()] = v.strip()
    return out

_sanitize = lambda t: re.sub(r"\W+", "_", t.strip()).strip("_")

# ---------------------------------------------------------------------------
# ID derivation
# ---------------------------------------------------------------------------


def _derive_ids(
    data: bytes,
    fname: str,
    p0: Optional[str],
    s0: Optional[str],
    e0: Optional[str],
    sc0: Optional[int],
    d0: Optional[str],              
) -> Tuple[str, str, str, int, str, List[str]]:
    """
    Pull identifying information from an RDA header.

    Returns
    -------
    project, subject, session, scan_id, study_date, msgs
    """
    msgs: List[str] = []
    p, s, e, sc, d = p0, s0, e0, sc0, d0          # ← initialise all targets

    if fname.lower().endswith(".rda"):
        hdr = _parse_hdr(data)

        # ------------------------------------------------------------------
        # Project  (XNAT project = StudyDescription)
        # ------------------------------------------------------------------
        if p is None and (sd := hdr.get("StudyDescription")):
            p = _sanitize(sd)
            msgs.append(f"{fname}: Project   → '{p}' (StudyDescription)")

        # ------------------------------------------------------------------
        # Subject  (XNAT subject = PatientName)
        # ------------------------------------------------------------------
        if s is None and (pn := hdr.get("PatientName")):
            s = _sanitize(pn)
            msgs.append(f"{fname}: Subject   → '{s}' (PatientName)")

        # ------------------------------------------------------------------
        # Session  (XNAT experiment/session = PatientID)
        # ------------------------------------------------------------------
        if e is None and (pid := hdr.get("PatientID")):
            e = _sanitize(pid)
            msgs.append(f"{fname}: Session   → '{e}' (PatientID)")

        # ------------------------------------------------------------------
        # Scan ID  (XNAT scan = SeriesNumber)
        # ------------------------------------------------------------------
        if sc is None and (sn := hdr.get("SeriesNumber")):
            sc = int(sn)
            msgs.append(f"{fname}: ScanID    → {sc} (SeriesNumber)")

        # ------------------------------------------------------------------
        # StudyDate (DICOM tag 0008,0020)
        # ------------------------------------------------------------------
        if d is None and (sd_raw := hdr.get("StudyDate")):
                # keep raw if format is unexpected
                d = sd_raw
                msgs.append(f"{fname}: StudyDate → {d} ")

    return p, s, e, sc, d, msgs


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        # wipe any staged files from a prior session attempt
        _clear_all_staged_for_session()

        user = request.form['username'].strip()
        pwd = request.form['password']
        if _check_credentials(user, pwd):
            session.permanent = True
            session['xnat_user'], session['xnat_pass'] = user, pwd
            return redirect(url_for('index'))
        flash('Invalid username or password')
    return render_template_string(LOGIN_HTML)


@app.route('/logout')
def logout():
    _clear_all_staged_for_session()
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
def index():
    if 'xnat_user' not in session:
        return redirect(url_for('login'))
    reload_urls = session.pop('xnat_reload_urls', [])
    pending_rows = session.get("pending_rows", [])
    return render_template_string(
        UPLOAD_HTML,
        XNAT_BASE_URL=XNAT_BASE_URL,
        reload_urls=reload_urls,
        pending_rows=pending_rows,
    )

@app.route('/upload', methods=['POST'])
def upload():
    if "xnat_user" not in session:
        return redirect(url_for("login"))

    label = "MRS"

    # ---- Table arrays (these are the authoritative state) ----
    names = request.form.getlist('file_names')
    scans = request.form.getlist('scan_ids')
    descs = request.form.getlist('series_descs')
    projs = request.form.getlist('project_ids')
    subs  = request.form.getlist('subject_labels')
    exps  = request.form.getlist('experiment_labels')
    toks  = request.form.getlist('file_tokens')  # hidden input per row

    def _to_int_or_none(x: Optional[str]) -> Optional[int]:
        try:
            x = (x or "").strip()
            return int(x) if x.isdigit() else None
        except Exception:
            return None

    # ------------------------------------------------------------
    # Determine: first submit vs retry submit
    # ------------------------------------------------------------
    has_tokens = any(t.strip() for t in toks)

    if not has_tokens:
        # FIRST SUBMIT: stage incoming files
        incoming = [f for f in request.files.getlist("files") if f and f.filename]
        if not incoming:
            flash("No files selected.")
            return redirect(url_for("index"))

        pending_files: dict[str, dict] = {}
        for fs in incoming:
            token, path = _stage_save_filestorage(fs)
            pending_files[token] = {"filename": fs.filename, "path": path}
        session["pending_files"] = pending_files

        # map filename -> token (assumes filenames are unique per batch)
        fname_to_tok = {info["filename"]: tok for tok, info in pending_files.items()}
        toks = [fname_to_tok.get(fn, "") for fn in names]

    else:
        # RETRY SUBMIT: must have staged files
        if not session.get("pending_files"):
            flash("Staged file data expired. Please re-select your files and try again.")
            session.pop("pending_rows", None)
            return redirect(url_for("index"))

    # ------------------------------------------------------------
    # Build row items from table (persisted on failure)
    # ------------------------------------------------------------
    row_items: list[dict] = []
    for i, fname in enumerate(names):
        row_items.append({
            "token": (toks[i].strip() if i < len(toks) else ""),
            "filename": fname,
            "kind": "dat" if fname.lower().endswith(".dat") else "rda",
            "scan_id": _to_int_or_none(scans[i]) if i < len(scans) else None,
            "series_desc": (descs[i].strip() if i < len(descs) else ""),
            "project": _sanitize(projs[i]) if i < len(projs) and projs[i].strip() else "",
            "subject": _sanitize(subs[i])  if i < len(subs)  and subs[i].strip()  else "",
            "session": _sanitize(exps[i])  if i < len(exps)  and exps[i].strip()  else "",
            "key": "",  # optional
        })

    # Save current table state so the user never loses it
    session["pending_rows"] = row_items

    # ------------------------------------------------------------
    # Parse RDA metadata from STAGED bytes (needed for DAT matching)
    # ------------------------------------------------------------
    rda_meta: list[dict] = []
    for row in row_items:
        if row["kind"] != "rda":
            continue
        tok = row["token"]
        if not tok:
            continue
        try:
            with _staged_open(tok) as fh:
                blob = fh.read()
        except UserFacingError:
            # leave it to upload stage to error out nicely
            continue

        hdr = _parse_hdr(blob)
        p, s, e, sc, d, _ = _derive_ids(blob, row["filename"], None, None, None, None, None)

        rda_meta.append({
            "token": tok,
            "filename": row["filename"],
            "project": row["project"] or p,
            "subject": row["subject"] or s,
            "session": row["session"] or e,
            "scan_id": row["scan_id"] if row["scan_id"] is not None else sc,
            "series_desc": row["series_desc"] or hdr.get("SeriesDescription", ""),
            "study_date": d,
        })

    def best_match(dat_name: str, rdas: list[dict]) -> Optional[dict]:
        base = re.sub(r'[_-]', '', dat_name.lower())
        best = None
        for r in rdas:
            if not r.get("series_desc"):
                continue
            desc_norm = re.sub(r'[_-]', '', r["series_desc"].lower())
            score = 0
            if desc_norm in base or base in desc_norm:
                score += 2
            if r.get("scan_id") is not None and str(r["scan_id"]) in dat_name:
                score += 3
            if score and (not best or score > best[0]):
                best = (score, r)
        return best[1] if best else None

    # ------------------------------------------------------------
    # Upload per row; keep failures in pending_rows
    # ------------------------------------------------------------
    ok = 0
    msgs: list[str] = []
    bad: list[str] = []
    failed_rows: list[dict] = []
    seen_sessions = set()

    for row in row_items:
        tok = row["token"]
        name = row["filename"]

        if not tok:
            bad.append(f"{name}: Missing staged file token. Please re-select files and try again.")
            failed_rows.append(row)
            continue

        # Load staged bytes
        try:
            with _staged_open(tok) as fh:
                blob = fh.read()
        except UserFacingError as err:
            bad.append(f"{name}: {err}")
            failed_rows.append(row)
            continue

        # Determine metadata for this row
        if row["kind"] == "rda":
            meta = next((r for r in rda_meta if r["filename"] == name), None)
            if not meta:
                bad.append(f"{name}: Could not parse RDA metadata.")
                failed_rows.append(row)
                continue
        else:
            meta = best_match(name, rda_meta)
            if not meta:
                bad.append(f"{name}: No matching RDA found (cannot infer metadata).")
                failed_rows.append(row)
                continue

        # Apply table overrides (already sanitized)
        p = row["project"] or meta.get("project") or ""
        s = row["subject"] or meta.get("subject") or ""
        e = row["session"] or meta.get("session") or ""
        sc = row["scan_id"] if row["scan_id"] is not None else meta.get("scan_id")
        d = meta.get("study_date")

        if not (p and s and e and sc is not None):
            bad.append(f"{name}: Missing required Project/Subject/Session/Scan in the table.")
            failed_rows.append(row)
            continue

        # DICOM validation
        scan = f"/archive/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"
        try:
            img_type = dicom_field(XNAT_BASE_URL, scan, "ImageType")
            study_dt = dicom_field(XNAT_BASE_URL, scan, "StudyDate")
            series_description = dicom_field(XNAT_BASE_URL, scan, "SeriesDescription")
        except UserFacingError as err:
            logging.warning("DICOM validation failed for %s", name, exc_info=True)
            bad.append(f"{name}: {err}")
            failed_rows.append(row)
            continue

        if "SPECTROSCOPY" not in (img_type or "").upper():
            bad.append(f"{name}: Target DICOM '{series_description}' is not spectroscopy.")
            failed_rows.append(row)
            continue

        if study_dt != (d or study_dt):
            bad.append(f"{name}: StudyDate mismatch: DICOM={study_dt}, RDA={d}")
            failed_rows.append(row)
            continue

        # Upload
        base = f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"
        _ensure_resource(base, label)
        endpoint = f"{base}/resources/{label}/files/{urllib.parse.quote(name)}?inbody=true"

        result = _request("PUT", endpoint, blob)

        if result.ok:
            ok += 1
            seen_sessions.add((p, s, e))
            msgs.append(f"{name}: uploaded successfully (Scan {sc})")
            _staged_delete(tok)  # delete staged file on success
        else:
            if result.error_type == "network":
                bad.append(f"{name}: Network error uploading to XNAT. Please try again.")
            elif result.error_type == "timeout":
                bad.append(f"{name}: Upload timed out. Please try again.")
            elif result.error_type == "auth":
                bad.append(f"{name}: Authentication error uploading to XNAT. Please log in again.")
            else:
                code = result.status if result.status is not None else "unknown"
                bad.append(f"{name}: upload failed (HTTP {code})")
            failed_rows.append(row)

    # Persist only failed rows so the table keeps them for retry
    session["pending_rows"] = failed_rows

    # If nothing failed, clear pending state entirely
    if not failed_rows:
        session.pop("pending_rows", None)
        if not session.get("pending_files"):
            session.pop("pending_files", None)

    # Summary banner
    if ok and bad:
        flash(f"Upload finished with errors: {ok} uploaded, {len(bad)} failed.")
    elif ok:
        flash("Upload complete: all selected files were uploaded successfully.")
    else:
        flash("No files were uploaded. Please review the errors and try again.")

    for m in msgs:
        flash(m)
    for b in bad:
        flash(b)

    if ok:
        session["xnat_reload_urls"] = [
            f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
            for (p, s, e) in sorted(seen_sessions)
        ]

    return redirect(url_for("index"))



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--test', action='store_true')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=5000)
    args = ap.parse_args()
    if args.test:
        unittest.main(argv=[sys.argv[0]])
    else:
        app.run(host=args.host, port=args.port, debug=True)

if __name__ == '__main__':
    main()
