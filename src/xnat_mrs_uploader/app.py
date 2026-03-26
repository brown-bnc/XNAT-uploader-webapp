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
        make_response,
        get_flashed_messages
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
_active_requests = 0
_active_lock = threading.Lock()
_state_lock = threading.Lock()

def _touch_activity() -> None:
    global _last_activity
    _last_activity = time.time()

@app.before_request
def _idle_touch_before_request():
    global _active_requests

    if request.path not in ("/__healthz", "/__shutdown", "/__quit"):
        _touch_activity()

    with _active_lock:
        _active_requests += 1

@app.teardown_request
def _track_request_end(exc):
    global _active_requests
    with _active_lock:
        _active_requests = max(0, _active_requests - 1)

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
XNAT_HTTP_TIMEOUT = int(os.getenv("XNAT_HTTP_TIMEOUT", "1800"))   # seconds
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
    position: relative;
    background: white;
    padding: 2.5rem 3rem;
    border-radius: 8px;
    box-shadow: 0 4px 18px rgba(0,0,0,0.1);
    width: 100%;
    max-width: 400px;
  }

  .toolbar {
    position: absolute;
    top: 12px;
    right: 12px;
  }

  .btn-ghost {
    appearance: none;
    border: 1px solid #dfe8ff;
    background: #eef3ff;
    color: #4285f4;
    border-radius: 6px;
    padding: .45rem .7rem;
    font-weight: 700;
    cursor: pointer;
    font-size: .9rem;
  }
  .btn-ghost:hover { background:#e3ecff; border-color:#d2e0ff; }
  .btn-ghost:disabled { opacity: .6; cursor: default; }

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

  button.login {
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

  button.login:hover { background: #357ae8; }

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

    <div class="toolbar">
      <button type="button" id="quitBtn" class="btn-ghost">Quit</button>
    </div>

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

      <button class="login" type="submit">Log In</button>
    </form>

    <footer>Brown University • Behavior & Neurodata Core</footer>
  </div>

<script>
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
</script>
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
    --flash-bg:#eef3ff; --flash-border:#dfe8ff; --flash-text:#2f5fb3;
    --flash-err-bg:#fdecea; --flash-err-border:#f5c2c0; --flash-err-text:#b12a2a;
    --flash-success-bg:#edf8f0; --flash-success-border:#cfe8d5; --flash-success-text:#1f6b36;
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

  .flash {
    background:var(--flash-bg);
    border:1px solid var(--flash-border);
    color:var(--flash-text);
    padding:.55rem .75rem;
    border-radius:6px;
    margin:.5rem 0 .75rem;
    font-size:.88rem;
  }
  .flash-error {
    background:var(--flash-err-bg);
    border:1px solid var(--flash-err-border);
    color:var(--flash-err-text);
  }
  .flash-success {
    background:var(--flash-success-bg);
    border:1px solid var(--flash-success-border);
    color:var(--flash-success-text);
  }
  .flash-summary {
    font-size:.9rem;
    font-weight:600;
  }

  label { display:block; font-weight:600; margin:.6rem 0 .35rem; }
  input[type=text] {
    width:100%; padding:.6rem .75rem; border:1px solid var(--border); border-radius:6px; box-sizing:border-box; font-size:.98rem; background:#fff;
  }
  input[type=text]:focus { border-color:var(--primary); outline:none; box-shadow:0 0 0 2px rgba(66,133,244,.18); }
  .actions { display:flex; gap:.6rem; margin-top:1rem; flex-wrap:wrap; }
  button, .btn { appearance:none; border:none; border-radius:6px; padding:.65rem 1rem; font-weight:700; cursor:pointer; font-size:.98rem; }
  .btn-primary { background:var(--primary); color:#fff; } .btn-primary:hover{ background:var(--primary-hover); }
  .btn-ghost { background:#eef3ff; color:var(--primary); border:1px solid #dfe8ff; } .btn-ghost:hover{ background:#e3ecff; border-color:#d2e0ff; }

  .file-picker-row {
    display:flex;
    align-items:center;
    gap:.75rem;
    margin-top:.35rem;
    flex-wrap:wrap;
  }

  .table-wrap { margin-top:1rem; border:1px solid var(--border); border-radius:8px; overflow:hidden; background:#fff; }
  table.file-table { width:100%; border-collapse:collapse; table-layout:fixed; }
  table.file-table colgroup col:nth-child(1){width:33%;}
  table.file-table colgroup col:nth-child(2){width:5%;}
  table.file-table colgroup col:nth-child(3){width:21%;}
  table.file-table colgroup col:nth-child(4){width:10%;}
  table.file-table colgroup col:nth-child(5){width:9%;}
  table.file-table colgroup col:nth-child(6){width:9%;}
  table.file-table colgroup col:nth-child(7){width:5%;}
  table.file-table colgroup col:nth-child(8){width:8%;}
  thead th { text-align:left; background:#f5f7fb; color:#333; padding:.6rem .6rem; border-bottom:1px solid var(--border); font-weight:700; font-size:.8rem; }
  tbody td { border-top:1px solid var(--border); padding:.5rem .5rem; vertical-align:middle; background:#fff; }
  tbody td input[type=text]{ width:100%; padding:.45rem .55rem; border:1px solid #e6e6e6; border-radius:5px; font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; font-size:.8rem; }
  tbody td input[type=text]:focus{ border-color:var(--primary); box-shadow:0 0 0 2px rgba(66,133,244,.15); outline:none; }
  tbody td input[readonly]{ background:#fafafa; color:#444; }

  .status-cell{
    font-size:.78rem;
    font-weight:700;
    line-height:1.2;
    word-break:break-word;
    cursor: help;
  }

  .status-error{
    color:var(--flash-err-text);
  }
  tr.row-error td{
    background:#fff8f8;
  }

  .remove-btn{ display:inline-flex; align-items:center; justify-content:center; width:2rem; height:2rem; border:none; background:transparent; font-size:1.1rem; color:#a00; cursor:pointer; }
  .remove-btn:hover{ color:#d00; }
  #spinner-overlay { display:none; position:fixed; inset:0; background:rgba(255,255,255,.9); z-index:9999; justify-content:center; align-items:center; gap:.8rem; color:#333; font-weight:600; }
  .spinner { border:6px solid #ccd6ff; border-top:6px solid var(--primary); border-radius:50%; width:2.4rem; height:2.4rem; animation:spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .subtle { color:var(--muted); font-size:.9rem; margin:0; }
  .hidden-file-input { display:none; }
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

      {% with messages = get_flashed_messages(with_categories=true) %}
        {% for category, msg in messages %}
          <div class="flash{% if category == 'error' %} flash-error{% elif category == 'success' %} flash-success{% endif %} flash-summary">
            {{ msg }}
          </div>
        {% endfor %}
      {% endwith %}

      <form id="uploadForm" method="post" action="{{ url_for('upload') }}" enctype="multipart/form-data" novalidate>
        <input id="file_input" class="hidden-file-input" name="files" type="file" accept=".rda,.dat" multiple>

        <div class="file-picker-row">
          <button type="button" id="pickFilesBtn" class="btn btn-ghost">Select .rda and/or .dat files</button>
          <div id="fileCount" class="subtle">No files loaded</div>
        </div>

        <div class="subtle" style="margin-top:1.35rem;">
          TIP: RDA file headers are used to automatically determine Project, Subject, Session, & Scan.
          TWIX (.dat) files inherit this information only when matching is unambiguous.
          If this fails, you will need to manually enter it prior to uploading.
        </div>

        <div id="matchWarnings" class="flash flash-error" style="display:none;"></div>

        <div class="table-wrap">
          <table class="file-table">
            <colgroup><col><col><col><col><col><col><col><col></colgroup>
            <thead>
              <tr>
                <th>Filename</th>
                <th>Scan&nbsp;ID</th>
                <th>Series&nbsp;Description</th>
                <th>Project</th>
                <th>Subject</th>
                <th>Session</th>
                <th>Remove</th>
                <th>Upload Error</th>
              </tr>
            </thead>
            <tbody id="fileTableBody"></tbody>
          </table>
        </div>

        <div class="actions">
          <button type="button" id="preview_xnat_btn" class="btn btn-ghost">Preview XNAT Session(s)</button>
          <button type="submit" class="btn btn-primary">Upload</button>
        </div>
      </form>
    </div>
  </div>

  <div id="spinner-overlay"><div class="spinner"></div>Uploading…</div>

<script>
window.addEventListener('error', e => console.error('JS Error:', e.message, e.error));
function log(){ try{ console.log.apply(console, arguments); }catch(_){} }
function warn(){ try{ console.warn.apply(console, arguments); }catch(_){} }
function err(){ try{ console.error.apply(console, arguments); }catch(_){} }

let previewWins = [];
const rowByUid = new Map();
const selectedFiles = new Map();
const dirtyByUid = new Map();

function markDirty(uid, fieldName){
  if (!uid) return;
  let s = dirtyByUid.get(uid);
  if (!s){ s = new Set(); dirtyByUid.set(uid, s); }
  s.add(fieldName);
}
function isDirty(uid, fieldName){
  const s = uid ? dirtyByUid.get(uid) : null;
  return !!(s && s.has(fieldName));
}
function clearDirtyForUid(uid){
  if (!uid) return;
  dirtyByUid.delete(uid);
}

const rdaMetasByKey = new Map();
const datRowsByKey  = new Map();
const rdaRowsByKey  = new Map();
const seriesLabelByKey = new Map();

function fileId(f){ return `${f.name}|||${f.size}|||${f.lastModified}`; }

let matchingFrozen = false;
function freezeMatching(){
  matchingFrozen = true;
  setMatchWarnings([]);
}
function unfreezeMatching(){
  if (!matchingFrozen) return;
  matchingFrozen = false;
  recomputeAllMatchingAndWarnings();
}

function sanitize(txt){ return String(txt||'').trim().replace(/\\W+/g,'_').replace(/^_|_$/g,''); }
function normSeriesKey(txt){
  return String(txt||'').toLowerCase().replace(/[^a-z0-9]+/g, '');
}
function normDate8(x){
  const s = String(x || "").replace(/\\D+/g, "");
  return s.length >= 8 ? s.slice(0,8) : s;
}
function updateFileCount() {
  const count = document.querySelectorAll('#fileTableBody tr').length;
  const el = document.getElementById('fileCount');
  if (!el) return;
  el.textContent = count ? `${count} file${count === 1 ? '' : 's'} loaded` : 'No files loaded';
}

const DAT_DATE_STORE_KEY = "xnat_uploader_dat_studydate_by_uid_v1";
function _loadDatDateStore(){
  try{
    const raw = sessionStorage.getItem(DAT_DATE_STORE_KEY);
    return raw ? (JSON.parse(raw) || {}) : {};
  }catch(_){ return {}; }
}
function _saveDatDateStore(obj){
  try{ sessionStorage.setItem(DAT_DATE_STORE_KEY, JSON.stringify(obj || {})); }catch(_){}
}
function rememberDatStudyDate(uid, date8){
  uid = String(uid || "");
  const d = normDate8(date8 || "");
  if (!uid || !d) return;
  const store = _loadDatDateStore();
  store[uid] = d;
  _saveDatDateStore(store);
}
function recallDatStudyDate(uid){
  uid = String(uid || "");
  if (!uid) return "";
  const store = _loadDatDateStore();
  return normDate8(store[uid] || "");
}
function forgetDatStudyDate(uid){
  uid = String(uid || "");
  if (!uid) return;
  const store = _loadDatDateStore();
  if (store.hasOwnProperty(uid)){
    delete store[uid];
    _saveDatDateStore(store);
  }
}

function parseDatParts(name){
  const s = String(name||'');
  const m = s.match(/^meas_(MID\\d+)_?(FID\\d+)_([\\s\\S]+)\\.dat$/i);
  if (!m) return { mid:"", fid:"", series:"" };
  return { mid: m[1] || "", fid: m[2] || "", series: m[3] || "" };
}
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

function parseDatMetadataFromText(txt){
  const out = { series: "", studyDate: "" };
  if (!txt) return out;

  const protocolMatches = [];
  const protocolRe = /ParamString\\."tProtocolName"\\>\\{\\s*"([^"]+)"\\s*\\}/g;
  let m;

  while ((m = protocolRe.exec(txt)) !== null) {
    const val = String(m[1] || "").trim();
    if (!val) continue;
    if (/^initialized by sequence$/i.test(val)) continue;
    if (/^adjcoilsens$/i.test(val)) continue;
    protocolMatches.push(val);
  }

  if (protocolMatches.length) {
    const preferred = protocolMatches.find(
      v => /\\bmrs\\b/i.test(v) || /\\bsvs\\b/i.test(v) || /\\bpress\\b/i.test(v)
    );
    out.series = (preferred || protocolMatches[protocolMatches.length - 1] || "").trim();
  }

  const i = txt.indexOf('SeriesLOID');
  if (i >= 0) {
    const windowTxt = txt.slice(i, i + 1200);
    let loid = "";

    m = windowTxt.match(/SeriesLOID[\\s\\S]{0,400}?\\{\\s*"([^"]{10,240})"\\s*\\}/);
    if (m) loid = m[1];

    if (!loid){
      m = windowTxt.match(/SeriesLOID[\\s\\S]{0,400}?"([^"]{10,240})"/);
      if (m) loid = m[1];
    }

    if (loid){
      const d14 = loid.match(/(20\\d{12})/);
      if (d14) {
        out.studyDate = d14[1].slice(0, 8);
      } else {
        const d8 = loid.match(/(20\\d{6})/) || loid.match(/(\\d{8})/);
        if (d8) out.studyDate = d8[1];
      }
    }
  }

  out.studyDate = normDate8(out.studyDate);
  return out;
}

function readFileLatin1(file){
  return new Promise((resolve,reject)=>{
    const r = new FileReader();
    r.onload = e => {
      try {
        const buf = e.target.result;
        const bytes = new Uint8Array(buf);
        let s = "";
        const chunk = 0x8000;
        for (let i=0;i<bytes.length;i+=chunk){
          s += String.fromCharCode.apply(null, bytes.subarray(i, i+chunk));
        }
        resolve(s);
      } catch(err){ reject(err); }
    };
    r.onerror = reject;
    r.readAsArrayBuffer(file);
  });
}

function rowVals(row){
  return {
    project: ((row.querySelector("input[name='project_ids']")||{}).value || '').trim(),
    subject: ((row.querySelector("input[name='subject_labels']")||{}).value || '').trim(),
    session: ((row.querySelector("input[name='experiment_labels']")||{}).value || '').trim(),
    scan:    ((row.querySelector("input[name='scan_ids']")||{}).value || '').trim(),
    series:  ((row.querySelector("input[name='series_descs']")||{}).value || '').trim()
  };
}
function datHasTarget(row){
  const v = rowVals(row);
  return !!(v.scan || v.project || v.subject || v.session);
}
function maybeClearDatTargets(row){
  if (row && row.dataset && row.dataset.kind === "dat" && matchingFrozen && datHasTarget(row)) return;
  ["scan_ids","project_ids","subject_labels","experiment_labels"].forEach(fn => clearIfNotDirty(row, fn));
}
function setIfNotDirtyOverwrite(row, fieldName, value){
  const inp = row.querySelector(`input[name='${fieldName}']`);
  if (!inp) return;
  const uid = row.dataset.uid || "";
  if (isDirty(uid, fieldName)) return;
  const v = (value || '').toString();
  inp.value = v;
  inp.title = v;
}
function clearIfNotDirty(row, fieldName){
  const inp = row.querySelector(`input[name='${fieldName}']`);
  if (!inp) return;
  const uid = row.dataset.uid || "";
  if (isDirty(uid, fieldName)) return;
  inp.value = "";
  inp.title = "";
}
function setMatchWarnings(lines){
  const box = document.getElementById("matchWarnings");
  if (!box) return;
  if (!lines || !lines.length){
    box.style.display = "none";
    box.textContent = "";
    return;
  }
  box.style.display = "block";
  box.innerHTML = "<b>Matching issue:</b><br>" + lines.map(x => `• ${x}`).join("<br>");
}

function upsertRdaMeta(key, meta){
  if (!key) return;
  const arr = rdaMetasByKey.get(key) || [];
  const idx = arr.findIndex(x => x.uid && meta.uid && x.uid === meta.uid);
  if (idx >= 0) arr[idx] = meta;
  else arr.push(meta);
  rdaMetasByKey.set(key, arr);

  if (meta.series && String(meta.series).trim()){
    seriesLabelByKey.set(key, String(meta.series).trim());
  }
}
function removeRdaMeta(key, uid){
  const arr = rdaMetasByKey.get(key) || [];
  const next = arr.filter(x => !(x.uid && uid && x.uid === uid));
  if (next.length) rdaMetasByKey.set(key, next);
  else rdaMetasByKey.delete(key);
}

function updateMatchingForKey(key){
  if (!key) return [];
  const warnings = [];

  const datRows = datRowsByKey.get(key) || [];
  const rdaMetasAll = rdaMetasByKey.get(key) || [];
  const label = seriesLabelByKey.get(key) || key;

  function sameDerivedMeta(cands){
    if (!cands || cands.length < 2) return true;
    const norm = x => ({
      scan: String(x.scan||"").trim(),
      project: String(x.project||"").trim(),
      subject: String(x.subject||"").trim(),
      session: String(x.session||"").trim(),
    });
    const a = norm(cands[0]);
    for (let i=1;i<cands.length;i++){
      const b = norm(cands[i]);
      if (a.scan!==b.scan || a.project!==b.project || a.subject!==b.subject || a.session!==b.session) return false;
    }
    return true;
  }

  const datByDate = new Map();
  for (const row of datRows){
    const d = normDate8(row.dataset.datStudyDate || "");
    if (!d){
      if (!matchingFrozen) {
        warnings.push(`DAT "${row.dataset.filename || ''}" in series "${label}" could not be matched automatically because its StudyDate could not be read.`);
      }
      maybeClearDatTargets(row);
      continue;
    }
    const arr = datByDate.get(d) || [];
    arr.push(row);
    datByDate.set(d, arr);
  }

  for (const [datDate, groupRows] of datByDate.entries()){
    const candidates = rdaMetasAll.filter(r => normDate8(r.studyDate || "") === datDate);

    if (candidates.length === 0){
      for (const row of groupRows){
        if (!matchingFrozen) {
          warnings.push(`DAT "${row.dataset.filename || ''}" in series "${label}" could not be matched automatically because no RDA with StudyDate ${datDate} was found.`);
        }
        maybeClearDatTargets(row);
      }
      continue;
    }

    if (candidates.length > 3){
      for (const row of groupRows){
        if (!matchingFrozen) {
          warnings.push(`DAT "${row.dataset.filename || ''}" in series "${label}" could not be matched automatically because ${candidates.length} possible RDAs were found for StudyDate ${datDate}.`);
        }
        maybeClearDatTargets(row);
      }
      continue;
    }

    if (groupRows.length > candidates.length){
      for (const row of groupRows){
        if (!matchingFrozen) {
          warnings.push(`DAT "${row.dataset.filename || ''}" in series "${label}" could not be matched automatically because there are more DAT files than matching RDAs for StudyDate ${datDate}.`);
        }
        maybeClearDatTargets(row);
      }
      continue;
    }

    if (candidates.length > 1 && !sameDerivedMeta(candidates)){
      for (const row of groupRows){
        if (!matchingFrozen) {
          warnings.push(`DAT "${row.dataset.filename || ''}" in series "${label}" could not be matched automatically because the matching RDAs for StudyDate ${datDate} have different metadata.`);
        }
        maybeClearDatTargets(row);
      }
      continue;
    }

    const m = candidates[0];
    for (const row of groupRows){
      if (matchingFrozen && row.dataset.kind === "dat" && datHasTarget(row)) continue;
      setIfNotDirtyOverwrite(row, "scan_ids", m.scan || "");
      setIfNotDirtyOverwrite(row, "project_ids", m.project || "");
      setIfNotDirtyOverwrite(row, "subject_labels", m.subject || "");
      setIfNotDirtyOverwrite(row, "experiment_labels", m.session || "");
    }
  }

  return warnings;
}

function recomputeAllMatchingAndWarnings(){
  if (matchingFrozen){
    setMatchWarnings([]);
    return;
  }

  const warns = [];
  const allKeys = new Set();
  for (const k of rdaMetasByKey.keys()) allKeys.add(k);
  for (const k of datRowsByKey.keys()) allKeys.add(k);

  for (const key of allKeys){
    const w = updateMatchingForKey(key);
    if (w && w.length) warns.push(...w);
  }
  setMatchWarnings(warns);
}

function addRow(kind, fileName, meta){
  const tbody = document.getElementById('fileTableBody');
  if (!tbody){ err('tbody not found'); return null; }

  const uidVal = (meta.uid || '').trim();
  if (uidVal && rowByUid.has(uidVal)) return rowByUid.get(uidVal);

  const row = document.createElement('tr');
  row.dataset.kind = kind;
  row.dataset.seriesKey = meta.key || '';
  row.dataset.filename = fileName;
  row.dataset.uid = uidVal;

  const tok = document.createElement('input');
  tok.type = 'hidden';
  tok.name = 'file_tokens';
  tok.value = meta.token || '';
  row.appendChild(tok);

  const uid = document.createElement('input');
  uid.type = 'hidden';
  uid.name = 'row_uids';
  uid.value = uidVal;
  row.appendChild(uid);

  const dsd = document.createElement('input');
  dsd.type = 'hidden';
  dsd.name = 'dat_study_dates';
  dsd.value = meta.datStudyDate ? normDate8(meta.datStudyDate) : '';
  row.appendChild(dsd);

  function mkCell(name, val, readonly){
    const td = document.createElement('td');
    const inp = document.createElement('input');
    inp.type = 'text';
    inp.name = name;
    inp.value = val || '';
    inp.title = val || '';
    if (readonly) inp.readOnly = true;
    if (!readonly){
      inp.addEventListener('input', function(){
        markDirty(row.dataset.uid || "", name);
        if (row._statusCell && row._statusCell.textContent.trim()) {
          row._statusCell.textContent = '';
          row.classList.remove('row-error');
        }
        unfreezeMatching();
      });
    }
    td.appendChild(inp);
    return td;
  }

  function mkStatusCell(statusText, fullText){
    const td = document.createElement('td');

    const shortMsg = statusText || '';
    const fullMsg  = fullText || shortMsg;

    td.textContent = shortMsg;
    td.title = shortMsg ? fullMsg : '';

    if (shortMsg) {
      td.className = 'status-cell status-error';
      row.classList.add('row-error');
    } else {
      td.className = 'status-cell';
    }

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
    unfreezeMatching();

    const fileInput = document.getElementById('file_input');
    if (fileInput) fileInput.value = '';

    const key = row.dataset.seriesKey;
    const uid = row.dataset.uid;

    if (uid){
      rowByUid.delete(uid);
      selectedFiles.delete(uid);
      clearDirtyForUid(uid);
      forgetDatStudyDate(uid);
    }

    if (row.dataset.kind === 'rda'){
      const arr = rdaRowsByKey.get(key) || [];
      rdaRowsByKey.set(key, arr.filter(r => r !== row));
      if ((rdaRowsByKey.get(key) || []).length === 0) rdaRowsByKey.delete(key);
      removeRdaMeta(key, uid);
    }

    if (row.dataset.kind === 'dat'){
      const arr = datRowsByKey.get(key) || [];
      datRowsByKey.set(key, arr.filter(r => r !== row));
      if ((datRowsByKey.get(key) || []).length === 0) datRowsByKey.delete(key);
    }

    if (key){
      const remainingRdas = (rdaRowsByKey.get(key) || []).length;
      const remainingDats = (datRowsByKey.get(key) || []).length;
      if (remainingRdas + remainingDats === 0) seriesLabelByKey.delete(key);
    }

    row.remove();
    recomputeAllMatchingAndWarnings();
    updateFileCount();
  });

  tdRemove.appendChild(btn);
  row.appendChild(tdRemove);

  const initialStatus = meta.error || '';
  const initialStatusFull = meta.errorFull || initialStatus;
  const statusTd = mkStatusCell(initialStatus, initialStatusFull);
  row.appendChild(statusTd);
  row._statusCell = statusTd;

  tbody.appendChild(row);
  if (uidVal) rowByUid.set(uidVal, row);

  const key = row.dataset.seriesKey;

  if (key && meta.series && String(meta.series).trim()){
    if (!seriesLabelByKey.has(key)) seriesLabelByKey.set(key, String(meta.series).trim());
    if (kind === "rda") seriesLabelByKey.set(key, String(meta.series).trim());
  }

  if (kind === 'rda'){
    const arr = rdaRowsByKey.get(key) || [];
    arr.push(row);
    rdaRowsByKey.set(key, arr);

    upsertRdaMeta(key, {
      uid: uidVal,
      scan: (meta.scan || ""),
      project: (meta.project || ""),
      subject: (meta.subject || ""),
      session: (meta.session || ""),
      series: (meta.series || ""),
      studyDate: normDate8(meta.studyDate || "")
    });

    ['project_ids','subject_labels','experiment_labels','scan_ids','series_descs'].forEach(function(sel){
      const inp = row.querySelector("input[name='" + sel + "']");
      if (!inp) return;
      inp.addEventListener('input', function(){
        unfreezeMatching();

        const vals = rowVals(row);
        upsertRdaMeta(key, {
          uid: uidVal,
          scan: vals.scan || "",
          project: vals.project || "",
          subject: vals.subject || "",
          session: vals.session || "",
          series: vals.series || "",
          studyDate: normDate8(meta.studyDate || "")
        });
        if (vals.series && String(vals.series).trim()) seriesLabelByKey.set(key, String(vals.series).trim());
        recomputeAllMatchingAndWarnings();
      });
    });

  } else {
    const arr = datRowsByKey.get(key) || [];
    arr.push(row);
    datRowsByKey.set(key, arr);
  }

  let rehydrated = "";
  if (kind === "dat") {
    rehydrated = normDate8(meta.datStudyDate || "") || recallDatStudyDate(uidVal) || "";
    if (rehydrated) {
      row.dataset.datStudyDate = rehydrated;
      rememberDatStudyDate(uidVal, rehydrated);
    } else {
      delete row.dataset.datStudyDate;
    }
  } else {
    delete row.dataset.datStudyDate;
  }

  const dsdInp = row.querySelector("input[name='dat_study_dates']");
  if (dsdInp) dsdInp.value = rehydrated;

  recomputeAllMatchingAndWarnings();
  updateFileCount();
  return row;
}

async function handleFiles(fileList){
  try{
    const newlyPicked = Array.prototype.slice.call(fileList || []);
    log('Newly picked:', newlyPicked.map(f => f.name));

    for (const f of newlyPicked){
      selectedFiles.set(fileId(f), f);
    }

    const rdaFiles = newlyPicked.filter(f => /\\.rda$/i.test(f.name));
    for (const f of rdaFiles){
      const uid = fileId(f);
      if (rowByUid.has(uid)) continue;

      let txt = '';
      try { txt = await readFileText(f); }
      catch(ex){ warn('FileReader failed for', f.name, ex); continue; }

      const hdr = parseRDA(txt);
      const studyDate = normDate8(hdr.StudyDate || "");

      const meta = {
        scan:   (hdr.SeriesNumber || '').trim ? (hdr.SeriesNumber || '').trim() : (hdr.SeriesNumber || ''),
        series: (hdr.SeriesDescription || '').trim ? (hdr.SeriesDescription || '').trim() : (hdr.SeriesDescription || ''),
        project: sanitize(hdr.StudyDescription || ''),
        subject: sanitize(hdr.PatientName || ''),
        session: sanitize(hdr.PatientID || ''),
        key: normSeriesKey(hdr.SeriesDescription || ''),
        uid,
        studyDate,
        error: ''
      };

      addRow('rda', f.name, meta);
    }

    const datFiles = newlyPicked.filter(f => /\\.dat$/i.test(f.name));
    for (const f of datFiles){
      const uid = fileId(f);
      if (rowByUid.has(uid)) continue;

      let txt = "";
      try {
        txt = await readFileLatin1(f);
      } catch(ex) {
        warn('FileReader failed for', f.name, ex);
      }

      const parsed = parseDatMetadataFromText(txt);
      const parts = parseDatParts(f.name);
      const seriesRaw = parsed.series || parts.series || parseDatSeries(f.name);
      const key = normSeriesKey(seriesRaw);
      const datDate = normDate8(parsed.studyDate || "");

      if (datDate) rememberDatStudyDate(uid, datDate);

      addRow('dat', f.name, {
        scan: "",
        series: seriesRaw,
        project: "",
        subject: "",
        session: "",
        key,
        uid,
        datStudyDate: datDate,
        error: ''
      });
    }

    log('Rows now:', document.querySelectorAll('#fileTableBody tr').length);
    updateFileCount();
  } catch (e){
    err('handleFiles failed:', e);
    alert('Problem processing files. See console for details.');
  }
}

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

window.addEventListener('DOMContentLoaded', function(){
  const fileInput = document.getElementById('file_input');
  const pickFilesBtn = document.getElementById('pickFilesBtn');
  const form = document.getElementById('uploadForm');
  const tbody = document.getElementById('fileTableBody');
  if (!fileInput){ err('#file_input missing'); return; }

  if (pickFilesBtn) {
    pickFilesBtn.addEventListener('click', function(){
      fileInput.click();
    });
  }

  {% if pending_rows %}
  try {
    freezeMatching();

    const rows = {{ pending_rows | tojson }};
    tbody.innerHTML = '';

    rowByUid.clear();
    selectedFiles.clear();
    rdaMetasByKey.clear();
    datRowsByKey.clear();
    rdaRowsByKey.clear();
    seriesLabelByKey.clear();
    dirtyByUid.clear();

    rows.forEach(r => {
      const uid = r.uid || '';
      const restoredDatDate = normDate8(r.dat_study_date || "") || recallDatStudyDate(uid) || "";
      if (restoredDatDate) rememberDatStudyDate(uid, restoredDatDate);

      addRow(r.kind, r.filename, {
        scan: r.scan_id || '',
        series: r.series_desc || '',
        project: r.project || '',
        subject: r.subject || '',
        session: r.session || '',
        key: r.key || normSeriesKey(r.series_desc || ''),
        token: r.token || '',
        uid: uid,
        studyDate: r.study_date || "",
        datStudyDate: restoredDatDate,
        error: r.error || '',
        errorFull: r.error_full || ''
      });
    });

    setMatchWarnings([]);
    updateFileCount();

  } catch (e) {
    console.warn("Failed to restore pending rows", e);
  }
  {% endif %}

  fileInput.addEventListener('change', async function(e){
    unfreezeMatching();
    try {
      await handleFiles(e.target.files);
    } finally {
      e.target.value = '';
    }
  });

  if (tbody){
    tbody.addEventListener('input', function(){ unfreezeMatching(); }, { capture: true });
    tbody.addEventListener('click', function(ev){
      const b = ev.target && ev.target.closest && ev.target.closest('.remove-btn');
      if (b) unfreezeMatching();
    }, { capture: true });
  }

  const prevBtn = document.getElementById('preview_xnat_btn');
  if (prevBtn) prevBtn.addEventListener('click', openXNATPreviewsFromTable);

  if (form){
    form.addEventListener('submit', function(){
      try {
        document.querySelectorAll("input[name='upload_uids']").forEach(n => n.remove());

        const dt = new DataTransfer();
        const rows = document.querySelectorAll('#fileTableBody tr');

        rows.forEach(row => {
          const tok = (row.querySelector("input[name='file_tokens']")||{}).value || '';
          const uid = (row.querySelector("input[name='row_uids']")||{}).value || '';
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

        const fileInput = document.getElementById('file_input');
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

  updateFileCount();
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
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        return

    handler = RotatingFileHandler(
        logfile,
        maxBytes=2_000_000,   # ~2 MB
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
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
    pending = _get_server_state_value("pending_files", {}) or {}
    info = pending.get(token)
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
    pending = _get_server_state_value("pending_files", {}) or {}
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
    _set_server_state_value("pending_files", pending)

def _clear_all_staged_for_session() -> None:
    pending = _get_server_state_value("pending_files", {}) or {}
    for tok, info in list(pending.items()):
        path = info.get("path")
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except OSError:
            pass
        pending.pop(tok, None)

    _pop_server_state_value("pending_files", None)
    _pop_server_state_value("pending_rows", None)
    _pop_server_state_value("xnat_reload_urls", None)
    _delete_server_state_file()

def _session_state_dir() -> Path:
    d = Path.home() / ".xnat_uploader" / "session_state"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _get_state_id() -> str:
    sid = session.get("state_id")
    if not sid:
        sid = secrets.token_urlsafe(16)
        session["state_id"] = sid
    return sid

def _state_path() -> Path:
    return _session_state_dir() / f"{_get_state_id()}.json"

def _load_server_state() -> dict:
    with _state_lock:
        p = _state_path()
        if not p.exists():
            return {}
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            logging.warning("Failed reading session state file: %s", p, exc_info=True)
            return {}

def _save_server_state(state: dict) -> None:
    with _state_lock:
        p = _state_path()
        tmp = p.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, p)

def _get_server_state_value(key: str, default=None):
    state = _load_server_state()
    return state.get(key, default)

def _set_server_state_value(key: str, value) -> None:
    with _state_lock:
        p = _state_path()
        state = {}
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    state = loaded
            except Exception:
                logging.warning("Failed reading session state file: %s", p, exc_info=True)

        state[key] = value

        tmp = p.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, p)

def _pop_server_state_value(key: str, default=None):
    with _state_lock:
        p = _state_path()
        state = {}
        if p.exists():
            try:
                with open(p, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    state = loaded
            except Exception:
                logging.warning("Failed reading session state file: %s", p, exc_info=True)

        value = state.pop(key, default)

        tmp = p.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, p)
        return value

def _delete_server_state_file() -> None:
    with _state_lock:
        try:
            p = _state_path()
            if p.exists():
                p.unlink()
        except Exception:
            logging.warning("Failed deleting session state file", exc_info=True)

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
    result = _request("PUT", f"{base}/resources/{label}", data=b"")

    if result.ok:
        return
    
    # XNAT may return 409 when the resource already exists, but that's ok
    if result.status == 409:
        logging.info("Resource %s already exists at %s", label, base)
        return

    if result.error_type == "auth":
        raise UserFacingError(
            "XNAT session expired or you do not have access. Please log in again."
        )

    if result.error_type == "timeout":
        raise UserFacingError(
            "XNAT timed out while identifying the target destination."
        )

    if result.error_type == "network":
        raise UserFacingError(
            "Network error while identifying the target destination in XNAT."
        )

    if result.status is not None:
        raise UserFacingError(
            f"Unable to identify the target destination on XNAT (HTTP {result.status})."
        )

    raise UserFacingError(
        "Unable to identify the target destination on XNAT."
    )

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
            flash(str(err), "error")
            return render_template_string(LOGIN_HTML, shutdown_token=SHUTDOWN_TOKEN)

        session.permanent = True
        session['xnat_user'] = user
        session['xnat_jsession'] = js
        session.pop('xnat_pass', None)
        return redirect(url_for('index'))

    return render_template_string(LOGIN_HTML, shutdown_token=SHUTDOWN_TOKEN)



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
    reload_urls = _pop_server_state_value("xnat_reload_urls", [])
    pending_rows = _get_server_state_value("pending_rows", [])
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
    import logging
    import os
    import re
    import urllib.parse
    from typing import Optional

    label = "MRS"

    names = request.form.getlist("file_names")
    scans = request.form.getlist("scan_ids")
    descs = request.form.getlist("series_descs")
    projs = request.form.getlist("project_ids")
    subs  = request.form.getlist("subject_labels")
    exps  = request.form.getlist("experiment_labels")
    toks  = request.form.getlist("file_tokens")
    row_uids = request.form.getlist("row_uids")
    dat_study_dates = request.form.getlist("dat_study_dates")
    upload_uids = request.form.getlist("upload_uids")

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

    def _norm_date8(x: Optional[str]) -> str:
        s = re.sub(r"\D+", "", (x or ""))
        return s[:8] if len(s) >= 8 else s

    def _extract_twix_seriesloid_date(dat_bytes: bytes) -> Optional[str]:
        txt = dat_bytes.decode("latin-1", errors="ignore")

        m = re.search(r'SeriesLOID"\>\{\s*"([^"]+)"\s*\}', txt)
        if not m:
            m = re.search(r'(?s)SeriesLOID.{0,400}?\{\s*"([^"]+)"\s*\}', txt)
        if not m:
            m = re.search(r'(?s)SeriesLOID.{0,400}?"([^"]+)"', txt)
        if not m:
            return None

        loid = m.group(1)

        t14 = re.search(r"(20\d{12})", loid)
        if t14:
            return t14.group(1)[:8]

        t8 = re.search(r"(20\d{6})", loid) or re.search(r"(\d{8})", loid)
        return t8.group(1) if t8 else None

    def _short_error(msg: str) -> str:
        m = (msg or "").lower()

        if "missing project" in m or "missing required" in m or "missing project, subject, session, or scan" in m:
            return "Missing info"
        if "not spectroscopy" in m:
            return "Destination scan not spectroscopy"
        if "could not determine studydate" in m or "could not read studydate" in m:
            return "Missing date"
        if "studydate mismatch" in m or "date mismatch" in m:
            return "Date mismatch"
        if "network" in m:
            return "Network error"
        if "timed out" in m or "timeout" in m:
            return "Timed out"
        if "authentication" in m or "session expired" in m or "log in again" in m:
            return "Login error"
        if "staged file" in m or "file data is missing" in m or "missing staged file token" in m:
            return "File missing"
        if "target destination" in m or "resource" in m:
            return "XNAT error"
        if "http " in m:
            return "HTTP error"
        return "Upload failed"

    if not names:
        flash("No files selected.", "error")
        return redirect(url_for("index"))

    if len(toks) < len(names):
        toks = toks + [""] * (len(names) - len(toks))
    if len(row_uids) < len(names):
        row_uids = row_uids + [""] * (len(names) - len(row_uids))
    if len(dat_study_dates) < len(names):
        dat_study_dates = dat_study_dates + [""] * (len(names) - len(dat_study_dates))

    for i, fname in enumerate(names):
        if (toks[i] or "").strip():
            continue
        if not (row_uids[i] or "").strip():
            flash(
                f"{fname}: missing row metadata. Please refresh and re-select the file.",
                "error",
            )
            return redirect(url_for("index"))

    incoming = [f for f in request.files.getlist("files") if f and f.filename]

    if incoming and len(upload_uids) != len(incoming):
        flash("Upload failed: missing file metadata. Please refresh and try again.", "error")
        return redirect(url_for("index"))

    pending_files: dict[str, dict] = _get_server_state_value("pending_files", {}) or {}

    staged_by_uid: dict[str, list[tuple[str, dict]]] = {}
    for tok, info in pending_files.items():
        uid0 = (info.get("uid") or "").strip()
        if uid0:
            staged_by_uid.setdefault(uid0, []).append((tok, info))

    uid_to_tok: dict[str, str] = {}
    for uid0, items in staged_by_uid.items():
        uid_to_tok[uid0] = items[-1][0]

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

        reused_tok: Optional[str] = None
        if uid in staged_by_uid and file_hash:
            for existing_tok, existing_info in staged_by_uid[uid]:
                if existing_info.get("sha256") == file_hash:
                    reused_tok = existing_tok
                    break

        if reused_tok:
            try:
                os.remove(path)
            except OSError:
                pass
            uid_to_tok[uid] = reused_tok
            continue

        pending_files[token] = {
            "filename": fs.filename,
            "path": path,
            "uid": uid,
            "size": size,
            "sha256": file_hash,
        }
        staged_by_uid.setdefault(uid, []).append((token, pending_files[token]))
        uid_to_tok[uid] = token

    _set_server_state_value("pending_files", pending_files)

    for i in range(len(names)):
        if (toks[i] or "").strip():
            continue
        uid = (row_uids[i] or "").strip()
        if uid and uid in uid_to_tok:
            toks[i] = uid_to_tok[uid]

    row_items: list[dict] = []
    for i, fname in enumerate(names):
        kind = "dat" if fname.lower().endswith(".dat") else "rda"
        row_items.append({
            "token": (toks[i].strip() if i < len(toks) else ""),
            "uid": (row_uids[i].strip() if i < len(row_uids) else ""),
            "filename": fname,
            "kind": kind,
            "scan_id": _to_int_or_none(scans[i]) if i < len(scans) else None,
            "series_desc": (descs[i].strip() if i < len(descs) else ""),
            "project": _sanitize(projs[i]) if i < len(projs) and projs[i].strip() else "",
            "subject": _sanitize(subs[i]) if i < len(subs) and subs[i].strip() else "",
            "session": _sanitize(exps[i]) if i < len(exps) and exps[i].strip() else "",
            "key": "",
            "dat_study_date": _norm_date8(dat_study_dates[i]) if kind == "dat" else "",
            "study_date": "",
            "error": "",
            "error_full": "",
        })

    _set_server_state_value("pending_rows", row_items)

    def _mark_failed(row: dict, full_message: str) -> None:
        row["error_full"] = full_message
        row["error"] = _short_error(full_message)
        failed_rows.append(row)

    ok = 0
    failed_rows: list[dict] = []
    seen_sessions = set()

    for row in row_items:
        row["error"] = ""
        row["error_full"] = ""

        tok = row["token"]
        name = row["filename"]

        if not tok:
            _mark_failed(row, "Missing staged file token.")
            continue

        try:
            with _staged_open(tok) as fh:
                blob = fh.read()
        except UserFacingError as err:
            _mark_failed(row, f"{name}: {err}")
            continue

        p = row["project"]
        s = row["subject"]
        e = row["session"]
        sc = row["scan_id"]

        if not (p and s and e and sc is not None):
            _mark_failed(row, "Missing required Project/Subject/Session/Scan in the table.")
            continue

        file_study_date = ""

        if row["kind"] == "rda":
            try:
                hdr = _parse_hdr(blob)
            except Exception:
                hdr = {}
            file_study_date = _norm_date8(hdr.get("StudyDate"))
            row["study_date"] = file_study_date
        else:
            file_study_date = _norm_date8(row.get("dat_study_date"))
            if not file_study_date:
                file_study_date = _norm_date8(_extract_twix_seriesloid_date(blob))
                if file_study_date:
                    row["dat_study_date"] = file_study_date

        scan = f"/archive/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"
        try:
            img_type = dicom_field(XNAT_BASE_URL, scan, "ImageType")
            study_dt = dicom_field(XNAT_BASE_URL, scan, "StudyDate")
            series_description = dicom_field(XNAT_BASE_URL, scan, "SeriesDescription")
        except UserFacingError as err:
            logging.warning("DICOM validation failed for %s", name, exc_info=True)
            _mark_failed(row, f"{err}")
            continue

        if "SPECTROSCOPY" not in (img_type or "").upper():
            _mark_failed(row, f"Target DICOM '{series_description}' is not spectroscopy.")
            continue

        if not file_study_date:
            kind_label = "RDA" if row["kind"] == "rda" else "DAT"
            _mark_failed(row, f"Could not determine StudyDate from the {kind_label} file.")
            continue

        if _norm_date8(study_dt) != _norm_date8(file_study_date):
            _mark_failed(row, f"StudyDate mismatch: DICOM={study_dt}, file={file_study_date}")
            continue

        base = f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"

        try:
            _ensure_resource(base, label)
        except UserFacingError as err:
            _mark_failed(row, f"{err}")
            continue

        endpoint = f"{base}/resources/{label}/files/{urllib.parse.quote(name)}?inbody=true"
        result = _request("PUT", endpoint, blob)

        if result.ok:
            ok += 1
            seen_sessions.add((p, s, e))
            _staged_delete(tok)
        else:
            if result.error_type == "network":
                full_msg = f"Upload failed due to network error."
            elif result.error_type == "timeout":
                full_msg = f"Upload failed due to timeout."
            elif result.error_type == "auth":
                full_msg = f"Upload failed due to authentication error. Please log in again."
            else:
                code = result.status if result.status is not None else "unknown"
                full_msg = f"Upload failed (HTTP {code})."
            _mark_failed(row, full_msg)

    _set_server_state_value("pending_rows", failed_rows)

    if not failed_rows:
        _pop_server_state_value("pending_rows", None)

        remaining_pending = _get_server_state_value("pending_files", {}) or {}
        if not remaining_pending:
            _pop_server_state_value("pending_files", None)

    bad = len(failed_rows)

    if ok and bad:
        flash(f"Upload finished: {ok} uploaded, {bad} failed.", "error")
    elif ok:
        flash(f"Upload complete: {ok} file(s) uploaded.", "success")
    else:
        flash("No files were uploaded.", "error")

    if ok:
        _set_server_state_value("xnat_reload_urls", [
            f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
            for (p, s, e) in sorted(seen_sessions)
        ])

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
            with _active_lock:
                busy = _active_requests > 0

            if not busy and (time.time() - _last_activity > idle_seconds):
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
