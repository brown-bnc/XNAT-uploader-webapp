# app.py — Flask web‑app for bulk uploading *.rda* resources to XNAT
# ====================================================================
"""Drag‑and‑drop Siemens `.rda` files straight into XNAT

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
from datetime import timedelta, datetime
from typing import Dict, List, Optional, Tuple
import json
import time
from socket import timeout as SocketTimeout



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
        {% for msg in messages %}<div class="flash">{{ msg|safe }}</div>{% endfor %}
      {% endwith %}

      <form id="uploadForm" method="post" action="{{ url_for('upload') }}" enctype="multipart/form-data" novalidate>
        <label for="rda_input">Select .rda and .dat file(s)</label>
        <input id="rda_input" name="files" type="file" accept=".rda,.dat" multiple required>

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

# UPLOAD_HTML = """
# <!doctype html><html lang=en><head><meta charset=utf-8>
# <title>Raw Spectroscopy Data XNAT Uploader</title>
# <style>
#   body {
#     font-family: sans-serif;
#     max-width: 90rem;
#     margin: 2rem auto;
#     padding: 0 2rem;
#   }

#   label { display: block; margin-top: 0.6rem; }
#   input[type=text], input[type=file] {
#     width: 100%;
#     padding: 0.45rem;
#     box-sizing: border-box;
#   }

#   button {
#     margin-top: 1.2rem;
#     padding: 0.6rem 1.2rem;
#     font-size: 1rem;
#     cursor: pointer;
#   }

#   .flash {
#     background: #eef;
#     border: 1px solid #ccd;
#     padding: 0.8rem;
#     margin-top: 1rem;
#   }

#   /* Spinner overlay */
#   #spinner-overlay {
#     display: none;
#     position: fixed;
#     inset: 0;
#     background: rgba(255,255,255,0.85);
#     z-index: 9999;
#     justify-content: center;
#     align-items: center;
#     font-size: 1.25rem;
#     color: #333;
#     gap: 0.75rem;
#   }

#   .spinner {
#     border: 6px solid #ccc;
#     border-top: 6px solid #333;
#     border-radius: 50%;
#     width: 2.5rem;
#     height: 2.5rem;
#     animation: spin 1s linear infinite;
#   }
#   @keyframes spin { to { transform: rotate(360deg); } }

#   /* File table */
#   table.file-table {
#     width: 100%;
#     border-collapse: collapse;
#     table-layout: fixed;
#     margin-top: 1rem;
#   }
#   table.file-table colgroup col:nth-child(1) { width: 25%; }
#   table.file-table colgroup col:nth-child(2) { width: 8%; }
#   table.file-table colgroup col:nth-child(3) { width: 25%; }
#   table.file-table colgroup col:nth-child(4) { width: 10%; }
#   table.file-table colgroup col:nth-child(5) { width: 10%; }
#   table.file-table colgroup col:nth-child(6) { width: 10%; }
#   table.file-table colgroup col:nth-child(7) { width: 5%; }

#   table.file-table th,
#   table.file-table td {
#     border: 1px solid #ccc;
#     padding: 0.4rem;
#     vertical-align: middle;
#   }

#   table.file-table th {
#     background: #f7f7f7;
#     text-align: left;
#   }

#   table.file-table input[type="text"] {
#     width: 100%;
#     padding: 0.3rem;
#     font-family: monospace;
#     overflow: hidden;
#     text-overflow: ellipsis;
#     white-space: nowrap;
#   }

#   table.file-table input[readonly] {
#     background: #f8f8f8;
#   }

#   .remove-btn {
#     display: inline-flex;
#     align-items: center;
#     justify-content: center;
#     width: 2rem;
#     height: 2rem;
#     border: none;
#     background: transparent;
#     font-size: 1.2rem;
#     color: #a00;
#     cursor: pointer;
#   }
#   .remove-btn:hover { color: #d00; }
# </style>

# <script>
# let previewWins = [];
# // Indexes to propagate RDA edits to matched DAT rows
# const rdaRowByKey = new Map();   // seriesKey -> <tr> (RDA)
# const datRowsByKey = new Map();  // seriesKey -> [<tr>, ...] (DATs)

# // ---------- utility helpers ----------
# function sanitize(txt) {
#   return txt.trim().replace(/\\W+/g, "_").replace(/^_|_$/g, "");
# }
# function normSeriesKey(txt){ return (txt||"").toLowerCase().replace(/[_-]/g,""); }
# function parseDatSeries(name){
#   // meas_<MID>_<FID>_<series desc>.dat
#   const m = name.match(/^meas_[^_]+_[^_]+_(.+)\\.dat$/i);
#   return m ? m[1] : "";
# }
# function readFileText(file){
#   return new Promise((resolve,reject)=>{
#     const r = new FileReader();
#     r.onload = e => resolve(String(e.target.result||""));
#     r.onerror = reject;
#     r.readAsText(file);
#   });
# }
# function parseRDA(text){
#   const start = '>>> Begin of header <<<';
#   const end   = '>>> End of header <<<';
#   const a = text.indexOf(start), b = text.indexOf(end);
#   if (a === -1 || b === -1) return {};
#   const lines = text.slice(a + start.length, b).split(/\\r?\\n/);
#   const dict = {};
#   for (const ln of lines) {
#     const i = ln.indexOf(':');
#     if (i > 0) dict[ln.slice(0, i).trim()] = ln.slice(i + 1).trim();
#   }
#   return dict;
# }

# // ---------- table row helpers ----------
# function rowVals(row){
#   return {
#     project: row.querySelector("input[name='project_ids']").value.trim(),
#     subject: row.querySelector("input[name='subject_labels']").value.trim(),
#     session: row.querySelector("input[name='experiment_labels']").value.trim(),
#     scan:    row.querySelector("input[name='scan_ids']").value.trim(),
#   };
# }

# function propagateFromRda(key){
#   const src = rdaRowByKey.get(key);
#   if (!src) return;
#   const vals = rowVals(src);
#   const targets = datRowsByKey.get(key) || [];
#   for (const row of targets){
#     // only update DAT fields that aren't marked dirty
#     for (const [name, value] of Object.entries({
#       project_ids: vals.project,
#       subject_labels: vals.subject,
#       experiment_labels: vals.session,
#       scan_ids: vals.scan,
#     })){
#       const inp = row.querySelector(`input[name='${name}']`);
#       if (!inp) continue;
#       if (inp.dataset.dirty === "1") continue;
#       inp.value = value;
#       inp.title = value;
#     }
#   }
# }

# function addRow(kind, fileName, meta){
#   // kind: 'rda' or 'dat'
#   // meta: {scan, series, project, subject, session, key}
#   const tbody = document.getElementById('fileTableBody');
#   const row = document.createElement('tr');
#   row.dataset.kind = kind;
#   row.dataset.seriesKey = meta.key || "";
#   row.dataset.filename = fileName;

#   function mkCell(name, val, readonly=false){
#     const td = document.createElement('td');
#     const inp = document.createElement('input');
#     inp.type = 'text';
#     inp.name = name;
#     inp.value = val || '';
#     inp.title = val || '';
#     if (readonly) inp.readOnly = true;
#     // mark DAT fields as dirty if user edits so we don't overwrite
#     if (kind === 'dat' && !readonly){
#       inp.addEventListener('input', ()=> { inp.dataset.dirty = '1'; });
#     }
#     td.appendChild(inp);
#     return td;
#   }

#   row.appendChild(mkCell('file_names', fileName, true));
#   row.appendChild(mkCell('scan_ids', meta.scan ?? ''));
#   row.appendChild(mkCell('series_descs', meta.series ?? ''));
#   row.appendChild(mkCell('project_ids', meta.project ?? ''));
#   row.appendChild(mkCell('subject_labels', meta.subject ?? ''));
#   row.appendChild(mkCell('experiment_labels', meta.session ?? ''));

#   // remove button
#   const tdRemove = document.createElement('td');
#   const btn = document.createElement('button');
#   btn.type = 'button';
#   btn.className = 'remove-btn';
#   btn.textContent = '❌';
#   btn.addEventListener('click', ()=> {
#     const key = row.dataset.seriesKey;
#     if (row.dataset.kind === 'rda'){ rdaRowByKey.delete(key); }
#     if (row.dataset.kind === 'dat'){
#       const arr = datRowsByKey.get(key) || [];
#       datRowsByKey.set(key, arr.filter(r => r !== row));
#     }
#     row.remove();
#   });
#   tdRemove.appendChild(btn);
#   row.appendChild(tdRemove);

#   tbody.appendChild(row);

#   // wire RDA edits → propagate to DATs
#   const key = row.dataset.seriesKey;
#   if (kind === 'rda'){
#     rdaRowByKey.set(key, row);
#     for (const sel of ["project_ids","subject_labels","experiment_labels","scan_ids"]){
#       row.querySelector(`input[name='${sel}']`).addEventListener('input', ()=>{
#         propagateFromRda(key);
#       });
#     }
#     // initial propagate
#     propagateFromRda(key);
#   } else {
#     const arr = datRowsByKey.get(key) || [];
#     arr.push(row);
#     datRowsByKey.set(key, arr);
#   }
# }

# // ---------- file handling ----------
# async function handleFiles(fileList){
#   const files = Array.from(fileList);

#   // reset table + indexes
#   document.getElementById('fileTableBody').innerHTML = '';
#   rdaRowByKey.clear(); datRowsByKey.clear();

#   // 1) Parse all RDAs first
#   const rdaFiles = files.filter(f => f.name.toLowerCase().endsWith('.rda'));
#   const rdaInfos = [];
#   for (const f of rdaFiles){
#     const txt = await readFileText(f);
#     const hdr = parseRDA(txt);
#     const meta = {
#       scan: (hdr.SeriesNumber||'').trim(),
#       series: (hdr.SeriesDescription||'').trim(),
#       project: sanitize(hdr.StudyDescription||''),
#       subject: sanitize(hdr.PatientName||''),
#       session: sanitize(hdr.PatientID||''),
#       key: normSeriesKey(hdr.SeriesDescription||''),
#     };
#     rdaInfos.push({ file: f, meta });
#   }
#   // add RDA rows
#   for (const {file, meta} of rdaInfos){
#     addRow('rda', file.name, meta);
#   }

#   // map for quick RDA lookup by normalized series key
#   const rdaMetaByKey = new Map(
#     rdaInfos.filter(x => x.meta.key).map(x => [x.meta.key, x.meta])
#   );

#   // 2) Now add DAT rows, inheriting from the matched RDA series (if found)
#   const datFiles = files.filter(f => f.name.toLowerCase().endsWith('.dat'));
#   for (const f of datFiles){
#     const seriesRaw = parseDatSeries(f.name);
#     const key = normSeriesKey(seriesRaw);
#     const rdaMeta = rdaMetaByKey.get(key) || {
#       scan:'', series:seriesRaw, project:'', subject:'', session:'', key
#     };
#     const meta = {
#       scan: rdaMeta.scan,
#       series: rdaMeta.series || seriesRaw,
#       project: rdaMeta.project,
#       subject: rdaMeta.subject,
#       session: rdaMeta.session,
#       key
#     };
#     addRow('dat', f.name, meta);
#   }
# }

# // ---------- preview (opens tabs based on current table values) ----------
# function openXNATPreviewsFromTable() {
#   const rows = document.querySelectorAll('#fileTableBody tr');
#   const uniq = new Set();
#   for (const row of rows) {
#     const p = row.querySelector("input[name='project_ids']").value.trim();
#     const s = row.querySelector("input[name='subject_labels']").value.trim();
#     const e = row.querySelector("input[name='experiment_labels']").value.trim();
#     if (p && s && e) uniq.add(`${p}|||${s}|||${e}`);
#   }
#   const xnatBase = {{ XNAT_BASE_URL | tojson }};
#   previewWins = [];
#   for (const key of uniq) {
#     const [p, s, e] = key.split("|||");
#     const url = `${xnatBase.replace(/\\/$/,'')}/data/projects/${encodeURIComponent(p)}/subjects/${encodeURIComponent(s)}/experiments/${encodeURIComponent(e)}`;
#     const w = window.open(url, '_blank');
#     if (w) previewWins.push(w);
#   }
# }

# // ---------- DOM ready ----------
# window.addEventListener('DOMContentLoaded', () => {
#   const fileInput = document.getElementById('rda_input');
#   const form = document.getElementById('uploadForm');

#   fileInput.addEventListener('change', e => { handleFiles(e.target.files); });
#   document.getElementById('preview_xnat_btn').addEventListener('click', openXNATPreviewsFromTable);

#   form.addEventListener('submit', () => {
#     if (previewWins && previewWins.length) {
#       previewWins.forEach(w => { try { if (w && !w.closed) w.close(); } catch {} });
#     }
#     document.getElementById('spinner-overlay').style.display = 'flex';
#   });

#   {% if reload_urls %}
#   try {
#     const urls = {{ reload_urls | tojson }};
#     for (const url of urls) {
#       const win = window.open(url, '_blank');
#       if (win) win.focus();
#     }
#   } catch (e) { console.warn('XNAT reload failed:', e); }
#   {% endif %}
# });
# </script>
# </head><body>
#   <p style="float:right"><a href="{{url_for('logout')}}">Logout</a></p>
#   <h2>Upload .rda and .dat files to XNAT</h2>

#   {% with messages=get_flashed_messages() %}
#     {% for msg in messages %}
#       <div class="flash">{{ msg|safe }}</div>
#     {% endfor %}
#   {% endwith %}

#   <form id="uploadForm" method="post" action="{{url_for('upload')}}" enctype="multipart/form-data">
#     <label>Select .rda and .dat file(s)
#       <input id="rda_input" name="files" type="file" accept=".rda,.dat" multiple required>
#     </label>

#     <table class="file-table">
#       <colgroup><col><col><col><col><col><col><col></colgroup>
#       <thead>
#         <tr>
#           <th>Filename</th>
#           <th>Scan&nbsp;ID</th>
#           <th>Series&nbsp;Description</th>
#           <th>Project</th>
#           <th>Subject</th>
#           <th>Session</th>
#           <th></th>
#         </tr>
#       </thead>
#       <tbody id="fileTableBody"></tbody>
#     </table>

#     <button type="button" id="preview_xnat_btn">👁 Preview XNAT Session(s)</button>
#     <button type="submit">Upload</button>
#   </form>

#   <div id="spinner-overlay">
#     <div class="spinner"></div>
#     Uploading…
#   </div>
# </body></html>
# """


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
    return 200 <= _request('GET', url, None, auth=(user, pwd)) < 300

def _request(method: str, url: str, data: bytes | None,
             auth: Tuple[str, str] | None = None,
             timeout: int | None = None) -> int:
    """Return HTTP status code. On network error, return 599."""
    user, pwd = auth if auth else _session_creds()
    req = urllib.request.Request(
        url, data=data, method=method,
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
                return resp.status
        except urllib.error.HTTPError as err:
            # HTTPError is a valid HTTP response (e.g. 4xx/5xx) — return code
            return err.code 
        except (urllib.error.URLError, SocketTimeout) as err:
            # Transient network/timeout: retry unless last attempt
            if attempt < tries - 1:
                time.sleep((backoff ** attempt))
                continue
            # Signal network failure with 599 (like curl's 5xx-ish sentinel)
            return 599

def _ensure_resource(base: str, label: str) -> None:
    _request("PUT", f"{base}/resources/{label}", data=b"")

# ---------------------------------------------------------------------------
# NEW helper that returns the decoded JSON body
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
        return json.load(resp)          # ← json.load() reads & parses in one go


# ---------------------------------------------------------------------------
# Convenience wrapper for the dicomdump call
# ---------------------------------------------------------------------------
def dicom_field(base: str, scan_src: str, field: str,
                auth: Tuple[str, str] | None = None) -> str:
    """
    Return the raw value of a DICOM tag (e.g. ImageType, StudyDate)
    for a particular scan in XNAT.
    """
    query = urllib.parse.urlencode({"src": scan_src, "field": field})  # handles & safely
    result = _request_json(f"{base}/data/services/dicomdump?{query}", auth=auth)
    return result["ResultSet"]["Result"][0]["value"]



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

# def _norm_series_key(txt: str) -> str:
#     """
#     Return a normalised key where underscores, dashes and case
#     differences no longer matter, e.g.
#         'mrs-mrsref_acq-PRESS_voi-Lacc'  ->
#         'mrsmrsrefacqpressvoilacc'
#     """
#     return re.sub(r'[_-]', '', txt).lower()

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
    d0: Optional[str],              # ← NEW: expected StudyDate in YYYYMMDD (or None)
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
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
def index():
    if 'xnat_user' not in session:
        return redirect(url_for('login'))
    reload_urls = session.pop('xnat_reload_urls', [])
    return render_template_string(UPLOAD_HTML, XNAT_BASE_URL=XNAT_BASE_URL, reload_urls=reload_urls)

@app.route('/upload', methods=['POST'])
def upload():
    if "xnat_user" not in session:
        return redirect(url_for("login"))

    rp = request.form
    p0, s0, e0, sc0, d0 = (
        rp.get("project_id") or None,
        rp.get("subject_label") or None,
        rp.get("experiment_label") or None,
        rp.get("scan_id") or None,
        rp.get("study_date") or None,
    )
    label = "MRS"
    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        flash("No files selected.")
        return redirect(url_for("index"))

    ok, bad, msgs = 0, [], []
    rda_meta: list[dict] = []

    # ------------------------------------------------------------------
    # 1) Parse all RDAs first, capture per-file metadata
    # ------------------------------------------------------------------
    for f in files:
        if not f.filename.lower().endswith(".rda"):
            continue
        blob = f.read()
        hdr = _parse_hdr(blob)
        p, s, e, sc, d, _ = _derive_ids(blob, f.filename, p0, s0, e0, sc0, d0)
        desc = hdr.get("SeriesDescription", "")
        rda_meta.append({
            "filename": f.filename,
            "project": p,
            "subject": s,
            "session": e,
            "scan_id": sc,
            "series_desc": desc,
            "study_date": d,
        })
        try:
            f.stream.seek(0)
        except Exception:
            try: f.seek(0)
            except Exception: pass

    # --- Per-row overrides from the table (arrays align by row index) ---
    names = request.form.getlist('file_names')
    scans = request.form.getlist('scan_ids')
    descs = request.form.getlist('series_descs')
    projs = request.form.getlist('project_ids')
    subs  = request.form.getlist('subject_labels')
    exps  = request.form.getlist('experiment_labels')

    def _to_int_or_none(x: Optional[str]) -> Optional[int]:
        try:
            return int(x) if x and str(x).strip().isdigit() else None
        except Exception:
            return None

    overrides: dict[str, dict] = {}
    for i, fname in enumerate(names):
        ov = {
            'project': (projs[i].strip() if i < len(projs) and projs[i].strip() else None),
            'subject': (subs[i].strip()  if i < len(subs)  and subs[i].strip()  else None),
            'session': (exps[i].strip()  if i < len(exps)  and exps[i].strip()  else None),
            'scan_id': _to_int_or_none(scans[i].strip()) if i < len(scans) else None,
            'series_desc': (descs[i].strip() if i < len(descs) and descs[i].strip() else None),
        }
        # sanitize IDs to match your existing behavior
        if ov['project']: ov['project'] = _sanitize(ov['project'])
        if ov['subject']: ov['subject'] = _sanitize(ov['subject'])
        if ov['session']: ov['session'] = _sanitize(ov['session'])
        overrides[fname] = ov

    # ------------------------------------------------------------------
    # 2) Helper for DAT ↔ RDA matching
    # ------------------------------------------------------------------
    def best_match(dat_name: str, rdas: list[dict]) -> Optional[dict]:
        """Find best matching RDA entry for a DAT filename."""
        base = re.sub(r'[_-]', '', dat_name.lower())
        best = None
        for r in rdas:
            if not r["series_desc"]:
                continue
            desc_norm = re.sub(r'[_-]', '', r["series_desc"].lower())
            score = 0
            if desc_norm in base or base in desc_norm:
                score += 2
            if str(r["scan_id"]) in dat_name:
                score += 3
            if score and (not best or score > best[0]):
                best = (score, r)
        return best[1] if best else None

    # ------------------------------------------------------------------
    # 3) Upload all files
    # ------------------------------------------------------------------
    seen_sessions = set()
    for f in files:
        name = f.filename
        ext = name.lower().rsplit(".", 1)[-1]
        blob = f.read()

        # ---------------- RDA ----------------
        if ext == "rda":
            meta = next((r for r in rda_meta if r["filename"] == name), None)
            if not meta:
                bad.append(f"{name}: could not parse RDA metadata")
                continue

        # ---------------- DAT ----------------
        elif ext == "dat":
            match = best_match(name, rda_meta)
            if not match:
                bad.append(f"{name}: no matching RDA found")
                continue
            meta = match

        # ---------------- Unsupported ----------------
        else:
            bad.append(f"{name}: unsupported extension")
            continue

        # Apply per-row overrides by filename (if provided)
        ov = overrides.get(name, {})  # name is f.filename
        p  = ov.get('project')     or meta['project']
        s  = ov.get('subject')     or meta['subject']
        e  = ov.get('session')     or meta['session']
        sc = ov.get('scan_id')     if ov.get('scan_id') is not None else meta['scan_id']
        d  = meta['study_date']    # keep your parsed StudyDate for checks
        desc = ov.get('series_desc') or meta['series_desc']

        # p, s, e, sc, d, desc = (
        #     meta["project"],
        #     meta["subject"],
        #     meta["session"],
        #     meta["scan_id"],
        #     meta["study_date"],
        #     meta["series_desc"],
        # )

        # ------------------------------------------------------------------
        # DICOM validation: confirm spectroscopy + matching date
        # ------------------------------------------------------------------
        scan = f"/archive/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"
        try:
            img_type = dicom_field(XNAT_BASE_URL, scan, "ImageType")
            study_dt = dicom_field(XNAT_BASE_URL, scan, "StudyDate")
            series_description = dicom_field(XNAT_BASE_URL, scan, "SeriesDescription")
        except Exception as err:
            bad.append(f"{name}: failed to read DICOM metadata ({err})")
            continue

        if "SPECTROSCOPY" not in img_type.upper():
            bad.append(f"{name}: Target DICOM '{series_description}' is not spectroscopy (ImageType='{img_type}')")
            continue

        if study_dt != (d or study_dt):
            bad.append(f"{name}: StudyDate mismatch — DICOM={study_dt}, RDA={d}")
            continue

        # ------------------------------------------------------------------
        # Upload
        # ------------------------------------------------------------------
        base = f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
        if sc:
            base += f"/scans/{sc}"

        _ensure_resource(base, label)
        endpoint = f"{base}/resources/{label}/files/{urllib.parse.quote(name)}?inbody=true"
        status = _request("PUT", endpoint, blob)

        if 200 <= status < 300:
            ok += 1
            seen_sessions.add((p, s, e))
            msgs.append(f"{name}: uploaded successfully (Scan {sc})")
        else:
            bad.append(f"{name}: upload failed (HTTP {status})")

    # ------------------------------------------------------------------
    # Flash results + reopen XNAT tabs
    # ------------------------------------------------------------------
    for m in msgs:
        flash(m)
    if ok:
        session["xnat_reload_urls"] = [
            f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
            for (p, s, e) in sorted(seen_sessions)
        ]
        flash("<strong>Upload complete.</strong> Files successfully uploaded.")
    if bad:
        flash("<br>".join(bad))

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
