#app.py — Flask web‑app for bulk uploading .rda & .dat spectroscopy resources to XNAT
# ====================================================================

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
from datetime import timedelta, datetime
from typing import Dict, List, Optional, Tuple, Any
import json
import time
from socket import timeout as SocketTimeout
from dataclasses import dataclass
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import secrets
import shutil
from http.cookies import SimpleCookie
import threading
import webbrowser
import requests
import ipaddress


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
        make_response
    )
except ModuleNotFoundError:
    print("ERROR: Flask not installed →  python -m pip install Flask")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
XNAT_BASE_URL: str = os.getenv("XNAT_BASE_URL", "https://xnat.bnc.brown.edu").rstrip("/")
app = Flask(__name__, instance_relative_config=True)


# ---------------------------------------------------------------------------
# Idle shutdown (auto-stop server when unused)
# ---------------------------------------------------------------------------
_last_activity = time.time()

def _touch_activity() -> None:
    global _last_activity
    _last_activity = time.time()

@app.before_request
def _idle_touch_before_request():
    # Don't count internal endpoints; optionally ignore static assets too.
    if request.path in ("/__healthz", "/__shutdown"):
        return
    _touch_activity()

@app.get("/__healthz")
def __healthz():
    return "ok"

SHUTDOWN_TOKEN = secrets.token_urlsafe(16)

def _is_loopback(addr: str | None) -> bool:
    if not addr:
        return False
    if addr.startswith("::ffff:"):
        addr = addr.split("::ffff:", 1)[1]
    try:
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False
    
@app.post("/__shutdown")
def __shutdown():
    # 1) only accept loopback (but don't 403 loudly)
    if not _is_loopback(request.remote_addr):
        return ("", 204)

    # 2) token gate so old tabs can't affect a new run
    tok = request.args.get("t") or request.headers.get("X-Shutdown-Token")
    if tok != SHUTDOWN_TOKEN:
        # IMPORTANT: don't 403; just ignore so Chrome doesn't show an error page
        return ("", 204)

    # cleanup (best-effort)
    try:
        _clear_all_staged_for_session()
    except Exception:
        pass
    try:
        _cleanup_orphaned_staged_files(max_age_hours=0)
    except Exception:
        pass

    func = request.environ.get("werkzeug.server.shutdown")
    if func is not None:
        func()
        return "shutting down (werkzeug)"

    def _exit_soon():
        time.sleep(0.2)
        os._exit(0)

    threading.Thread(target=_exit_soon, daemon=True).start()
    return "shutting down (forced)"

@app.post("/__quit")
def __quit():
    # Only accept loopback
    if not _is_loopback(request.remote_addr):
        return ("", 204)

    # Token gate (ignore old tabs)
    tok = request.args.get("t") or request.headers.get("X-Shutdown-Token")
    if tok != SHUTDOWN_TOKEN:
        return ("", 204)

    # Best-effort cleanup
    try:
        _clear_all_staged_for_session()
    except Exception:
        pass

    session.clear()

    resp = make_response("quitting")
    # Flask 3+: app.session_cookie_name is gone; use config key
    resp.delete_cookie(app.config.get("SESSION_COOKIE_NAME", "session"))

    # Shut down AFTER the response gets out
    func = request.environ.get("werkzeug.server.shutdown")
    if func is not None:
        threading.Thread(target=lambda: (time.sleep(0.2), func()), daemon=True).start()
        return resp

    # Fallback: hard-exit process
    threading.Thread(target=lambda: (time.sleep(0.2), os._exit(0)), daemon=True).start()
    return resp


    
def load_or_create_secret() -> str:
    env = os.getenv("FLASK_SECRET_KEY")
    if env and env.strip():
        return env.strip()

    return secrets.token_urlsafe(64)


app.secret_key = load_or_create_secret()

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=False,  # local HTTP
)
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
        <div class="toolbar">
  <button type="button" id="quitBtn" class="btn btn-ghost">Quit</button>
</div>

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
const rdaRowByKey = new Map();      // key -> rda <tr>
const datRowsByKey = new Map();     // key -> [dat <tr>, ...]
const rowByUid = new Map();         // uid -> <tr> (allows duplicate filenames)
const rdaMetaByKey = new Map();     // key -> {scan,project,subject,session,series,key}  (series kept for RDA row only)

/* ===== Accumulated local file selection (across multiple chooser actions) ===== */
const selectedFiles = new Map();    // uid -> File

function fileId(f){
  return `${f.name}|||${f.size}|||${f.lastModified}`;
}

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
    try { r.readAsText(file, 'ISO-8859-1'); } catch (_) { r.readAsText(file); }
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
    project: ((row.querySelector("input[name='project_ids']")||{}).value || '').trim(),
    subject: ((row.querySelector("input[name='subject_labels']")||{}).value || '').trim(),
    session: ((row.querySelector("input[name='experiment_labels']")||{}).value || '').trim(),
    scan:    ((row.querySelector("input[name='scan_ids']")||{}).value || '').trim(),
    series:  ((row.querySelector("input[name='series_descs']")||{}).value || '').trim()
  };
}

// Initial fill: only fill if empty AND not dirty
function setIfEmptyAndNotDirty(row, fieldName, value){
  const inp = row.querySelector(`input[name='${fieldName}']`);
  if (!inp) return;
  if (inp.dataset.dirty === "1") return;
  if ((inp.value || '').trim()) return;  // only fill empty
  const v = (value || '').toString();
  inp.value = v;
  inp.title = v;
}

// Propagation: overwrite if NOT dirty (even if non-empty)
function setIfNotDirtyOverwrite(row, fieldName, value){
  const inp = row.querySelector(`input[name='${fieldName}']`);
  if (!inp) return;
  if (inp.dataset.dirty === "1") return;
  const v = (value || '').toString();
  inp.value = v;
  inp.title = v;
}

// overwrite=false: initial fill (empty only)
// overwrite=true : propagate edits from RDA (overwrite non-dirty)
// NOTE: We intentionally do NOT touch series_descs for DAT rows.
function refreshDatMatchesForKey(key, overwrite){
  const meta = rdaMetaByKey.get(key);
  if (!meta) return;
  const rows = datRowsByKey.get(key) || [];
  const setter = overwrite ? setIfNotDirtyOverwrite : setIfEmptyAndNotDirty;

  for (const row of rows){
    setter(row, 'scan_ids', meta.scan ? String(meta.scan) : '');
    setter(row, 'project_ids', meta.project ? String(meta.project) : '');
    setter(row, 'subject_labels', meta.subject ? String(meta.subject) : '');
    setter(row, 'experiment_labels', meta.session ? String(meta.session) : '');
    // intentionally NOT updating 'series_descs'
  }
}

function propagateFromRda(key){
  const src = rdaRowByKey.get(key);
  if (!src) return;
  const vals = rowVals(src);

  // keep meta map in sync with edits to RDA row
  rdaMetaByKey.set(key, { ...vals, key });

  // propagate edits to DAT rows (overwrite non-dirty)
  refreshDatMatchesForKey(key, true);
}

function addRow(kind, fileName, meta){
  const tbody = document.getElementById('fileTableBody');
  if (!tbody){ err('tbody not found'); return; }

  const uidVal = (meta.uid || '').trim();

  // Prevent duplicates by UID (allows duplicate filenames)
  if (uidVal && rowByUid.has(uidVal)) return;

  const row = document.createElement('tr');
  row.dataset.kind = kind;
  row.dataset.seriesKey = meta.key || '';
  row.dataset.filename = fileName;
  row.dataset.uid = uidVal;

  // hidden token: used to re-load staged bytes on retry (server-side)
  const tok = document.createElement('input');
  tok.type = 'hidden';
  tok.name = 'file_tokens';
  tok.value = meta.token || '';
  row.appendChild(tok);

  // hidden uid per row: backend uses to map row -> staged file even with duplicate filenames
  const uid = document.createElement('input');
  uid.type = 'hidden';
  uid.name = 'row_uids';
  uid.value = uidVal;
  row.appendChild(uid);

  function mkCell(name, val, readonly){
    const td = document.createElement('td');
    const inp = document.createElement('input');
    inp.type = 'text';
    inp.name = name;
    inp.value = val || '';
    inp.title = val || '';
    if (readonly) inp.readOnly = true;

    // Track manual edits on all editable cells (so we don't overwrite them later)
    if (!readonly){
      inp.addEventListener('input', function(){ inp.dataset.dirty = '1'; });
    }

    td.appendChild(inp);
    return td;
  }

  row.appendChild(mkCell('file_names', fileName, true));
  row.appendChild(mkCell('scan_ids', meta.scan ? String(meta.scan) : '', false));
  row.appendChild(mkCell('series_descs', meta.series ? String(meta.series) : '', false));
  row.appendChild(mkCell('project_ids', meta.project ? String(meta.project) : '', false));
  row.appendChild(mkCell('subject_labels', meta.subject ? String(meta.subject) : '', false));
  row.appendChild(mkCell('experiment_labels', meta.session ? String(meta.session) : '', false));

  const tdRemove = document.createElement('td');
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'remove-btn';
  btn.textContent = '❌';
  btn.addEventListener('click', function(){
    const key = row.dataset.seriesKey;
    const uid = row.dataset.uid;

    if (uid){
      rowByUid.delete(uid);
      selectedFiles.delete(uid); // local file (no-op for token-only rows)
    }

    if (row.dataset.kind === 'rda'){
      rdaRowByKey.delete(key);
      rdaMetaByKey.delete(key);
    }
    if (row.dataset.kind === 'dat'){
      const arr = datRowsByKey.get(key) || [];
      datRowsByKey.set(key, arr.filter(r => r !== row));
    }

    row.remove();
  });
  tdRemove.appendChild(btn);
  row.appendChild(tdRemove);

  tbody.appendChild(row);
  if (uidVal) rowByUid.set(uidVal, row);

  const key = row.dataset.seriesKey;

  if (kind === 'rda'){
    rdaRowByKey.set(key, row);

    // seed meta map from row
    const vals = rowVals(row);
    if (key) rdaMetaByKey.set(key, { ...vals, key });

    // propagate on edits
    ['project_ids','subject_labels','experiment_labels','scan_ids','series_descs'].forEach(function(sel){
      const inp = row.querySelector("input[name='" + sel + "']");
      if (inp) inp.addEventListener('input', function(){ propagateFromRda(key); });
    });

    // initial fill of any existing DATs (empty-only, not series)
    if (key) refreshDatMatchesForKey(key, false);
  } else {
    const arr = datRowsByKey.get(key) || [];
    arr.push(row);
    datRowsByKey.set(key, arr);

    // if we already know an RDA for this key, fill any missing dat fields now (empty-only, not series)
    if (key) refreshDatMatchesForKey(key, false);
  }
}

/* ===== File handling: additive (DO NOT CLEAR TABLE) ===== */
async function handleFiles(fileList){
  try{
    const newlyPicked = Array.prototype.slice.call(fileList || []);
    log('Newly picked:', newlyPicked.map(f => f.name));

    // accumulate local files by uid
    for (const f of newlyPicked){
      selectedFiles.set(fileId(f), f);
    }

    // RDAs first
    const rdaFiles = newlyPicked.filter(f => /\\.rda$/i.test(f.name));
    for (const f of rdaFiles){
      const uid = fileId(f);
      if (rowByUid.has(uid)) continue;

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
        key: normSeriesKey(hdr.SeriesDescription || ''),
        uid
      };

      addRow('rda', f.name, meta);

      // seed meta map for matching later DAT rows (and fill existing dats empty-only)
      if (meta.key){
        rdaMetaByKey.set(meta.key, {
          scan: meta.scan || '',
          series: meta.series || '',   // kept but NOT propagated to DATs
          project: meta.project || '',
          subject: meta.subject || '',
          session: meta.session || '',
          key: meta.key
        });
        refreshDatMatchesForKey(meta.key, false);
      }
    }

    // DATs next
    const datFiles = newlyPicked.filter(f => /\\.dat$/i.test(f.name));
    for (const f of datFiles){
      const uid = fileId(f);
      if (rowByUid.has(uid)) continue;

      const seriesRaw = parseDatSeries(f.name);
      const key = normSeriesKey(seriesRaw);
      const meta = rdaMetaByKey.get(key) || { scan:'', series:'', project:'', subject:'', session:'', key };

      // IMPORTANT: DAT series_descs comes from DAT filename parsing (seriesRaw), not RDA
      addRow('dat', f.name, {
        scan: meta.scan,
        series: seriesRaw,
        project: meta.project,
        subject: meta.subject,
        session: meta.session,
        key,
        uid
      });
    }

    log('Rows now:', document.querySelectorAll('#fileTableBody tr').length);
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
    rowByUid.clear(); rdaMetaByKey.clear();
    selectedFiles.clear(); // restored rows are server-staged; no local File objects

    rows.forEach(r => {
      addRow(r.kind, r.filename, {
        scan: r.scan_id || '',
        series: r.series_desc || '',
        project: r.project || '',
        subject: r.subject || '',
        session: r.session || '',
        key: r.key || normSeriesKey(r.series_desc || ''),
        token: r.token || '',
        uid: r.uid || '' // IMPORTANT: backend must persist uid in pending_rows
      });
    });

    // Reconstruct minimal RDA meta from restored rows so later DATs can match,
    // and also so restored DATs can fill if their RDA is present.
    document.querySelectorAll('#fileTableBody tr').forEach(row => {
      if (row.dataset.kind !== 'rda') return;
      const key = row.dataset.seriesKey || '';
      if (!key) return;
      const vals = rowVals(row);
      rdaMetaByKey.set(key, { ...vals, key });
      refreshDatMatchesForKey(key, false); // empty-only on restore
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
      // Ensure the <input type="file"> includes ONLY locally-selected, NOT-yet-staged files,
      // and include upload_uids aligned with multipart order.
      try {
        // remove old upload_uids inputs
        document.querySelectorAll("input[name='upload_uids']").forEach(n => n.remove());

        const dt = new DataTransfer();
        const rows = document.querySelectorAll('#fileTableBody tr');

        rows.forEach(row => {
          const tok = (row.querySelector("input[name='file_tokens']")||{}).value || '';
          const uid = (row.querySelector("input[name='row_uids']")||{}).value || '';

          // if already staged (token exists), don't re-upload bytes
          if (tok && tok.trim()) return;

          const f = selectedFiles.get(uid);
          if (!f) return;

          dt.items.add(f);

          const u = document.createElement('input');
          u.type = 'hidden';
          u.name = 'upload_uids';
          u.value = uid;
          form.appendChild(u);
        });

        const fileInput = document.getElementById('rda_input');
        if (fileInput) fileInput.files = dt.files;
      } catch (e) {
        console.warn("Could not rebuild FileList for submit:", e);
      }

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


const shutdownToken = {{ shutdown_token | tojson }};

function requestShutdown() {
  try { navigator.sendBeacon(`/__quit?t=${encodeURIComponent(shutdownToken)}`); } catch(_) {}
  try {
    fetch(`/__quit?t=${encodeURIComponent(shutdownToken)}`, {
      method: "POST", cache: "no-store", credentials: "same-origin", keepalive: true
    }).catch(()=>{});
  } catch(_) {}
}


const quitBtn = document.getElementById("quitBtn");
if (quitBtn) {
  quitBtn.addEventListener("click", () => {
    quitBtn.disabled = true;
    quitBtn.textContent = "Quitting…";
    requestShutdown();

    document.body.innerHTML = `
      <div style="font-family:system-ui;padding:2rem">
        <h2>Uploader stopping…</h2>
      </div>`;
  });
}

});
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Backend helpers
# ---------------------------------------------------------------------------

def _get_jsession_from_response(resp, body_bytes: bytes) -> str:
    """
    XNAT may return the session id in the response body, and/or in Set-Cookie.
    Prefer Set-Cookie if present; fall back to body.
    """
    # 1) Try Set-Cookie
    set_cookie = resp.headers.get("Set-Cookie", "")
    if set_cookie:
        c = SimpleCookie()
        c.load(set_cookie)
        if "JSESSIONID" in c and c["JSESSIONID"].value:
            return c["JSESSIONID"].value.strip()

    # 2) Fall back to response body
    body = (body_bytes or b"").decode("utf-8", errors="ignore").strip()
    if body:
        return body

    raise UserFacingError("XNAT did not return a session token. Please try again.")


def _xnat_login_jsession(user: str, pwd: str, timeout: int = 15) -> str:
    """
    Exchange username/password for an XNAT JSESSIONID.
    Store only the JSESSIONID (not the password) in the Flask session.
    """
    url = f"{XNAT_BASE_URL}/data/JSESSION"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": _basic_auth_header(user, pwd),
            "Accept": "text/plain",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return _get_jsession_from_response(resp, body)

    except urllib.error.HTTPError as err:
        if err.code in (401, 403):
            raise UserFacingError("Invalid username/password or you do not have access to XNAT.") from err
        raise UserFacingError("XNAT returned an error while logging in. Please try again.") from err

    except (urllib.error.URLError, SocketTimeout) as err:
        raise UserFacingError("Unable to reach XNAT. Please check your network connection and try again.") from err


def _basic_auth_header(user: str, pwd: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _xnat_auth_headers(auth: Tuple[str, str] | None = None,
                       jsession: str | None = None) -> dict[str, str]:
    """
    Prefer JSESSIONID cookie auth (Option A). Allow Basic auth only when explicitly passed
    (e.g., during login to fetch JSESSIONID).
    """
    if auth is not None:
        user, pwd = auth
        return {"Authorization": _basic_auth_header(user, pwd)}
    js = jsession or session.get("xnat_jsession")
    if not js:
        raise RuntimeError("Not logged in")
    return {"Cookie": f"JSESSIONID={js}"}


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

def _stage_dir() -> Path:
    d = Path.home() / ".xnat_uploader" / "staged"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _cleanup_orphaned_staged_files(max_age_hours: int = 24) -> None:
    """
    Remove staged files left behind by crashed sessions / power loss / force quit.

    Deletes any file in ~/.xnat_uploader/staged older than max_age_hours.
    """
    d = _stage_dir()
    cutoff = time.time() - (max_age_hours * 3600)

    removed = 0
    for p in d.glob("*"):
        try:
            if not p.is_file():
                continue
            st = p.stat()
            if st.st_mtime < cutoff:
                p.unlink()
                removed += 1
        except Exception:
            logging.warning("Failed cleaning staged file: %s", p, exc_info=True)

    if removed:
        logging.info("Startup cleanup removed %d orphaned staged files", removed)

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


_init_logging()
_cleanup_orphaned_staged_files(max_age_hours=int(os.getenv("STAGED_FILE_MAX_AGE_HOURS", "24")))


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
             jsession: str | None = None,
             timeout: int | None = None) -> RequestResult:
    headers = {
        **_xnat_auth_headers(auth=auth, jsession=jsession),
        "Accept": "application/json",
    }

    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers=headers,
    )

    tries = max(1, XNAT_HTTP_RETRIES)
    to = timeout or XNAT_HTTP_TIMEOUT
    backoff = 1.5

    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=to) as resp:  # type: ignore[arg-type]
                return RequestResult(ok=True, status=resp.status)

        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                return RequestResult(
                    ok=False, status=err.code, error_type="auth",
                    message="XNAT session expired or you do not have access. Please log in again."
                )
            return RequestResult(
                ok=False, status=err.code, error_type="http",
                message=f"XNAT returned HTTP {err.code}."
            )

        except SocketTimeout:
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
                continue
            return RequestResult(ok=False, error_type="timeout",
                                 message="Network timeout while contacting XNAT.")

        except urllib.error.URLError:
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
                continue
            return RequestResult(ok=False, error_type="network",
                                 message="Network error while contacting XNAT.")

    return RequestResult(ok=False, error_type="unknown",
                         message="Unexpected error while contacting XNAT.")


def _ensure_resource(base: str, label: str) -> None:
    _request("PUT", f"{base}/resources/{label}", data=b"")

# ---------------------------------------------------------------------------
# helper that returns the decoded JSON body
# ---------------------------------------------------------------------------
def _request_json(url: str,
                  auth: Tuple[str, str] | None = None,
                  jsession: str | None = None,
                  timeout: int = 15) -> Any:
    headers = {
        **_xnat_auth_headers(auth=auth, jsession=jsession),
        "Accept": "application/json",
    }
    req = urllib.request.Request(url, headers=headers)
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
        _clear_all_staged_for_session()

        user = request.form['username'].strip()
        pwd = request.form['password']

        try:
            js = _xnat_login_jsession(user, pwd, timeout=15)
        except UserFacingError as err:
            flash(str(err))
            return render_template_string(LOGIN_HTML)

        session.permanent = True
        session['xnat_user'] = user          # optional (nice for auditing/logging)
        session['xnat_jsession'] = js        # ✅ store only this
        session.pop('xnat_pass', None)       # ✅ ensure no password sticks around
        return redirect(url_for('index'))

    return render_template_string(LOGIN_HTML)



@app.route('/logout')
def logout():
    _clear_all_staged_for_session()
    session.clear()
    return redirect(url_for('login'))

@app.after_request
def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

LAUNCH_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Launching XNAT Uploader…</title>
</head>
<body style="font-family:system-ui;padding:1.5rem">
  <h3>Launching XNAT Uploader…</h3>
  <p>If a popup blocker prevents the uploader window from opening, allow popups for this site and refresh.</p>

  <script>
    const shutdownToken = {{ shutdown_token | tojson }};

    // Open the real UI as a JS-opened window (closable later).
    // NOTE: must happen synchronously to avoid popup blockers.
    const child = window.open("/", "xnat_uploader", "popup=yes");

    // If popup blocked, give the user a clickable fallback.
    if (!child) {
      document.body.insertAdjacentHTML("beforeend", `
        <p><b>Popup was blocked.</b></p>
        <p><a href="/" target="_blank" rel="noopener">Open the uploader</a></p>
      `);
    }

    async function loop() {
      try {
        // Health check: no-store avoids cached "ok"
        const r = await fetch("/__healthz", { cache: "no-store" });
        if (!r.ok) throw new Error("health not ok");
      } catch (e) {
        // Server is gone -> close child window if we can
        try { if (child && !child.closed) child.close(); } catch (_) {}

        // Try to close launcher too (works if this tab was opened by code)
        try { window.close(); } catch (_) {}

        // If close was blocked, show a clear "stopped" page
        document.body.innerHTML = `
          <div style="font-family:system-ui;padding:2rem">
            <h2>Uploader stopped</h2>
            <p>The local server is no longer running.</p>
            <p>You can close this tab.</p>
          </div>`;
        return;
      }

      setTimeout(loop, 1200);
    }

    loop();
  </script>
</body>
</html>
"""
@app.get("/launch")
def launch():
    html = render_template_string(LAUNCH_HTML, shutdown_token=SHUTDOWN_TOKEN)
    return Response(html, headers={"Cache-Control": "no-store"})


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
        shutdown_token=SHUTDOWN_TOKEN,
    )


@app.route('/upload', methods=['POST'])
def upload():
    if "xnat_user" not in session:
        return redirect(url_for("login"))

    import hashlib

    label = "MRS"

    # ---- Table arrays (authoritative state) ----
    names = request.form.getlist('file_names')
    scans = request.form.getlist('scan_ids')
    descs = request.form.getlist('series_descs')
    projs = request.form.getlist('project_ids')
    subs  = request.form.getlist('subject_labels')
    exps  = request.form.getlist('experiment_labels')
    toks  = request.form.getlist('file_tokens')   # hidden input per row (may be blank)
    row_uids = request.form.getlist('row_uids')   # hidden input per row (may be blank for old sessions)

    # For the newly-uploaded multipart files only (order must match request.files.getlist("files"))
    upload_uids = request.form.getlist('upload_uids')

    def _to_int_or_none(x: Optional[str]) -> Optional[int]:
        try:
            x = (x or "").strip()
            return int(x) if x.isdigit() else None
        except Exception:
            return None

    def _sha256_file(path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    if not names:
        flash("No files selected.")
        return redirect(url_for("index"))

    # Normalize list lengths to avoid index errors
    if len(toks) < len(names):
        toks = toks + [""] * (len(names) - len(toks))
    if len(row_uids) < len(names):
        row_uids = row_uids + [""] * (len(names) - len(row_uids))

    # ------------------------------------------------------------
    # SAFETY: any row missing both token and uid is unmappable
    # ------------------------------------------------------------
    for i, fname in enumerate(names):
        if (toks[i] or "").strip():
            continue
        if not (row_uids[i] or "").strip():
            flash(
                f"Upload error: row '{fname}' is missing its metadata. "
                "Please refresh the page and re-select files."
            )
            return redirect(url_for("index"))

    # ------------------------------------------------------------
    # Stage any incoming files (works for BOTH first submit and retry)
    # Uses upload_uids to uniquely identify each incoming file, so
    # duplicate filenames are allowed.
    # ------------------------------------------------------------
    incoming = [f for f in request.files.getlist("files") if f and f.filename]

    # upload_uids must align with incoming files list order
    if incoming and len(upload_uids) != len(incoming):
        flash(
            "Upload error: missing file metadata. "
            "Please refresh the page and try again."
        )
        return redirect(url_for("index"))

    pending_files: dict[str, dict] = session.get("pending_files", {}) or {}

    # uid -> list of (token, info) in "most-recent-last" order
    staged_by_uid: dict[str, list[tuple[str, dict]]] = {}
    for tok, info in pending_files.items():
        uid0 = (info.get("uid") or "").strip()
        if uid0:
            staged_by_uid.setdefault(uid0, []).append((tok, info))

    # deterministically prefer the last staged entry for each uid
    uid_to_tok: dict[str, str] = {}
    for uid0, items in staged_by_uid.items():
        uid_to_tok[uid0] = items[-1][0]

    # Stage each incoming file; allow same filename as long as contents differ.
    # If the same uid arrives again:
    #   - if hash matches an already staged file under that uid, reuse its token
    #   - else stage a new entry (rare, but possible)
    for fs, uid in zip(incoming, upload_uids):
        uid = (uid or "").strip()
        if not uid:
            continue

        token, path = _stage_save_filestorage(fs)

        try:
            size = os.path.getsize(path)
        except Exception:
            size = None

        try:
            file_hash = _sha256_file(path)
        except Exception:
            file_hash = None

        # Check for identical content already staged under this uid
        reused_tok: Optional[str] = None
        if uid in staged_by_uid and file_hash:
            for existing_tok, existing_info in staged_by_uid[uid]:
                if existing_info.get("sha256") == file_hash:
                    reused_tok = existing_tok
                    break

        if reused_tok:
            # identical content already staged -> discard new file, set mapping, don't add entry
            try:
                os.remove(path)
            except OSError:
                pass
            uid_to_tok[uid] = reused_tok
            continue

        # Otherwise record a new staged file (even if filename duplicates)
        pending_files[token] = {
            "filename": fs.filename,
            "path": path,
            "uid": uid,
            "size": size,
            "sha256": file_hash,
        }
        staged_by_uid.setdefault(uid, []).append((token, pending_files[token]))
        uid_to_tok[uid] = token  # newest wins, deterministically

    session["pending_files"] = pending_files

    # ------------------------------------------------------------
    # Fill missing row tokens by UID (NOT filename)
    # ------------------------------------------------------------
    for i in range(len(names)):
        if (toks[i] or "").strip():
            continue
        uid = (row_uids[i] or "").strip()
        if uid and uid in uid_to_tok:
            toks[i] = uid_to_tok[uid]

    # ------------------------------------------------------------
    # Build row items from table (persisted on failure)
    # ------------------------------------------------------------
    row_items: list[dict] = []
    for i, fname in enumerate(names):
        row_items.append({
            "token": (toks[i].strip() if i < len(toks) else ""),
            "uid": (row_uids[i].strip() if i < len(row_uids) else ""),
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
            continue

        hdr = _parse_hdr(blob)
        p, s, e, sc, d, _ = _derive_ids(blob, row["filename"], None, None, None, None, None)

        rda_meta.append({
            "token": tok,
            "uid": row.get("uid", ""),
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
            bad.append(
                f"{name}: Missing staged file token. "
                "If you just added this file, please re-select it and try again."
            )
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
        meta = None
        has_rda_match = False

        if row["kind"] == "rda":
            meta = next((r for r in rda_meta if r["token"] == tok), None)
            if not meta:
                bad.append(f"{name}: Could not parse RDA metadata.")
                failed_rows.append(row)
                continue
            has_rda_match = True
        else:
            meta = best_match(name, rda_meta)
            if meta:
                has_rda_match = True
            else:
                # No RDA match. That's OK *if the table provides required identifiers*.
                # We'll proceed using table overrides only.
                has_rda_match = False
                meta = {}  # keep downstream code simple


        # Apply table overrides (already sanitized)
        p = row["project"] or meta.get("project") or ""
        s = row["subject"] or meta.get("subject") or ""
        e = row["session"] or meta.get("session") or ""
        sc = row["scan_id"] if row["scan_id"] is not None else meta.get("scan_id")
        d = meta.get("study_date")
        # If DAT has no RDA match, we can't validate StudyDate against RDA
        if row["kind"] == "dat" and not has_rda_match:
            d = None


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

        if d is not None and study_dt != d:
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
        if session.get("pending_files"):
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


def start_idle_shutdown_watchdog(host: str, port: int, idle_seconds: int) -> None:
    """
    Shut down the Flask dev server after `idle_seconds` with no requests.
    Uses localhost POST to /__shutdown so Werkzeug shutdown runs in request context.
    """
    if idle_seconds <= 0:
        return

    # Only works for local binds; if you bind 0.0.0.0 we still *try*,
    # but it’s best to keep local-only for safety.
    req_host = host
    if host in ("0.0.0.0", "::"):
        req_host = "127.0.0.1"

    health = f"http://{req_host}:{port}/__healthz"
    shutdown = f"http://{req_host}:{port}/__shutdown?t={SHUTDOWN_TOKEN}"

    def _watch():
        # Wait until server is reachable
        while True:
            try:
                requests.get(health, timeout=0.5)
                break
            except Exception:
                time.sleep(0.2)

        # Monitor idle time
        while True:
            time.sleep(2)
            if time.time() - _last_activity > idle_seconds:
                # _touch_activity()
                try:
                    print("trying to shut down server due to inactivity...")
                    r = requests.post(shutdown, timeout=1.0)
                    print("shutdown response:", r.status_code, r.text[:200])

                except Exception:
                    print('failed to shut down the server')
                    pass
                return

    threading.Thread(target=_watch, daemon=True).start()




# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        prog="xnat-mrs-uploader",
        description="Local Flask app for bulk uploading Siemens MRS (.rda/.dat) resources to XNAT",
    )
    ap.add_argument("--test", action="store_true", help="Run unit tests and exit")
    ap.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1 for local-only)")
    ap.add_argument("--port", type=int, default=5055, help="Port to listen on (default: 5055)")
    ap.add_argument("--debug", action="store_true", help="Enable Flask debug mode (default: off)")
    ap.add_argument(
        "--base-url",
        default=None,
        help="XNAT base URL (overrides XNAT_BASE_URL env var), e.g. https://xnat.bnc.brown.edu",
    )
    ap.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-open the web browser on startup",
    )
    ap.add_argument(
        "--idle-shutdown-seconds",
        type=int,
        default=600,
        help="Auto-stop server after N seconds of inactivity (default: 600). Set 0 to disable.",
    )

    args = ap.parse_args()

    if args.test:
        unittest.main(argv=[sys.argv[0]])
        return

    # Warn if binding to all interfaces
    if args.host in ("0.0.0.0", "::"):
        print(
            "WARNING: You are binding to all interfaces. Anyone who can reach this machine "
            "may be able to access the uploader. For local-only use, keep --host 127.0.0.1."
        )

    # Allow CLI override of XNAT_BASE_URL. Fall back to env var, then to the module default.
    global XNAT_BASE_URL
    if args.base_url and args.base_url.strip():
        XNAT_BASE_URL = args.base_url.strip().rstrip("/")
    else:
        XNAT_BASE_URL = os.getenv("XNAT_BASE_URL", XNAT_BASE_URL).rstrip("/")

    # Local HTTP defaults (safe for local runs; for HTTPS deployments you'd change these)
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=False,
    )
    url = f"http://{args.host}:{args.port}/launch"

    use_reloader = False

    if args.idle_shutdown_seconds > 0:
        start_idle_shutdown_watchdog(args.host, args.port, args.idle_shutdown_seconds)

    def maybe_open_browser() -> None:
        if args.no_browser:
            return
        if args.host not in ("127.0.0.1", "localhost"):
            return

        # If debug reloader is on, only open in the reloader child process.
        # If reloader is off, open in the single process.
        if (not use_reloader) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true"):
            threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    maybe_open_browser()
    app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=use_reloader)

if __name__ == '__main__':
    main()
