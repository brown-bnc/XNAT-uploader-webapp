# app.py — Flask web‑app for bulk uploading *.rda* resources to XNAT
# ====================================================================
"""Drag‑and‑drop Siemens `.rda` files straight into XNAT

Quick start
~~~~~~~~~~
```bash
python -m pip install Flask          # if not installed
pip install playwright
python -m playwright install chromium
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

# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------
LOGIN_HTML = """
<!doctype html>
<title>Login to XNAT</title>
<h2>XNAT login</h2>
{% with m=get_flashed_messages() %}{% for msg in m %}<p style='color:red'>{{msg}}</p>{% endfor %}{% endwith %}
<form method=post>
  <label>Username <input name=username required></label><br>
  <label>Password <input name=password type=password required></label><br>
  <button type=submit>Log in</button>
</form>
"""

UPLOAD_HTML = """
<!doctype html><html lang=en><head><meta charset=utf-8>
<title>Raw Spectroscopy Data XNAT Uploader</title>
<style>
 body{font-family:sans-serif;max-width:42rem;margin:2rem auto}
 label{display:block;margin-top:.6rem}
 input[type=text],input[type=file]{width:100%;padding:.45rem}
 button{margin-top:1.2rem;padding:.6rem 1.2rem;font-size:1rem}
 .flash{background:#eef;border:1px solid #ccd;padding:.8rem;margin-top:1rem}
 #xnat_snapshot_container img {border:1px solid #ccc; width:100%; max-width:100%;}
</style>
<script>
function sanitize(txt){return txt.trim().replace(/\\W+/g,'_').replace(/^_|_$/g,'');}
function parseRDA(text){
  const start='>>> Begin of header <<<'; const end='>>> End of header <<<';
  const a=text.indexOf(start), b=text.indexOf(end);
  if(a===-1||b===-1) return {};
  const lines = text.slice(a+start.length, b).split(/\\r?\\n/);
  const dict = {};
  for(const ln of lines){
    const i = ln.indexOf(':');
    if(i>0) dict[ln.slice(0,i).trim()] = ln.slice(i+1).trim();
  }
  return dict;
}
function applyHeader(h){
  if(h.StudyDescription) document.querySelector('[name=project_id]').value = sanitize(h.StudyDescription);
  if(h.PatientName)      document.querySelector('[name=subject_label]').value = sanitize(h.PatientName);
  if(h.PatientID)        document.querySelector('[name=experiment_label]').value = sanitize(h.PatientID);
  if(h.SeriesNumber)     document.querySelector('[name=scan_id]').value = h.SeriesNumber.trim();
}
function handleFiles(files){
  for(const f of files){
    if(f.name.toLowerCase().endsWith('.rda')){
      const reader = new FileReader();
      reader.onload = ev => applyHeader(parseRDA(ev.target.result));
      reader.readAsText(f);
      break;
    }
  }
}
window.addEventListener('DOMContentLoaded', () => {
  document.getElementById('rda_input').addEventListener('change', e => handleFiles(e.target.files));

  document.getElementById('xnat_preview_btn').addEventListener('click', () => {
    const p  = document.querySelector('[name=project_id]').value.trim();
    const s  = document.querySelector('[name=subject_label]').value.trim();
    const e  = document.querySelector('[name=experiment_label]').value.trim();
    if (!p || !s || !e) {
      alert('Please fill in Project, Subject, and Session first.');
      return;
    }

    // Show inline snapshot
    const q = new URLSearchParams({project: p, subject: s, experiment: e, stamp: Date.now()});
    const img = document.getElementById('xnat_snapshot_img');
    const container = document.getElementById('xnat_snapshot_container');
    img.src = '/preview_screenshot?' + q.toString();
    container.style.display = 'block';
  });
});
</script>
</head><body>
  <p style="float:right"><a href="{{url_for('logout')}}">Logout</a></p>
  <h2>Upload .rda and .dat files to XNAT</h2>
  {% with messages=get_flashed_messages() %}{% for msg in messages %}<div class=flash>{{msg|safe}}</div>{% endfor %}{% endwith %}
  <form method=post action="{{url_for('upload')}}" enctype=multipart/form-data>
    <label>Select .rda or .dat file(s) <input id="rda_input" name=files type=file accept=.rda,.dat multiple required></label>
    <label>Project ID <input name=project_id></label>
    <label>Subject Label <input name=subject_label></label>
    <label>Session (Experiment) Label <input name=experiment_label></label>
    <label>Scan ID <input name=scan_id></label>
    <label>Resource Label <input name=resource_label value=MRS required></label>
    <button type=submit>Upload</button>
  </form>

  <!-- Snapshot button and container -->
  <button type="button" id="xnat_preview_btn">📸 Preview XNAT Snapshot</button>

  <div id="xnat_snapshot_container" style="margin-top:1rem; display:none;">
    <h3>XNAT Preview Snapshot</h3>
    <img id="xnat_snapshot_img" src="" alt="XNAT Screenshot">
  </div>
</body></html>
"""

# UPLOAD_HTML = """
# <!doctype html><html lang=en><head><meta charset=utf-8>
# <title>Raw Spectroscopy Data XNAT Uploader</title>
# <style>
#  body{font-family:sans-serif;max-width:42rem;margin:2rem auto}
#  label{display:block;margin-top:.6rem}
#  input[type=text],input[type=file]{width:100%;padding:.45rem}
#  button{margin-top:1.2rem;padding:.6rem 1.2rem;font-size:1rem}
#  .flash{background:#eef;border:1px solid #ccd;padding:.8rem;margin-top:1rem}
# </style>
# <script>
# function sanitize(txt){return txt.trim().replace(/\\W+/g,'_').replace(/^_|_$/g,'');}
# function parseRDA(text){
#   const start='>>> Begin of header <<<'; const end='>>> End of header <<<';
#   const a=text.indexOf(start), b=text.indexOf(end);
#   if(a===-1||b===-1) return {};
#   const lines = text.slice(a+start.length, b).split(/\\r?\\n/);
#   const dict = {};
#   for(const ln of lines){
#     const i = ln.indexOf(':');
#     if(i>0) dict[ln.slice(0,i).trim()] = ln.slice(i+1).trim();
#   }
#   return dict;
# }
# function applyHeader(h){
#   if(h.StudyDescription) document.querySelector('[name=project_id]').value = sanitize(h.StudyDescription);
#   if(h.PatientName)      document.querySelector('[name=subject_label]').value = sanitize(h.PatientName);
#   if(h.PatientID)        document.querySelector('[name=experiment_label]').value = sanitize(h.PatientID);
#   if(h.SeriesNumber)     document.querySelector('[name=scan_id]').value = h.SeriesNumber.trim();
# }
# function handleFiles(files){
#   for(const f of files){
#     if(f.name.toLowerCase().endsWith('.rda')){
#       const reader = new FileReader();
#       reader.onload = ev => applyHeader(parseRDA(ev.target.result));
#       reader.readAsText(f);
#       break;
#     }
#   }
# }
# window.addEventListener('DOMContentLoaded', () => {
#   document.getElementById('rda_input').addEventListener('change', e => handleFiles(e.target.files));

#   // Preview button: redirect to /preview_inline
#   document.getElementById('xnat_preview_btn').addEventListener('click', () => {
#     const p  = document.querySelector('[name=project_id]').value.trim();
#     const s  = document.querySelector('[name=subject_label]').value.trim();
#     const e  = document.querySelector('[name=experiment_label]').value.trim();
#     if (!p || !s || !e) {
#       alert('Please fill in Project, Subject, and Session first.');
#       return;
#     }
#     const q = new URLSearchParams({project: p, subject: s, experiment: e});
#     window.location.href = '/preview_inline?' + q.toString();
#   });
# });
# </script>
# </head><body>
#   <p style="float:right"><a href="{{url_for('logout')}}">Logout</a></p>
#   <h2>Upload .rda and .dat files to XNAT</h2>
#   {% with messages=get_flashed_messages() %}{% for msg in messages %}<div class=flash>{{msg|safe}}</div>{% endfor %}{% endwith %}
#   <form method=post action="{{url_for('upload')}}" enctype=multipart/form-data>
#     <label>Select .rda or .dat file(s) <input id="rda_input" name=files type=file accept=.rda,.dat multiple required></label>
#     <label>Project ID <input name=project_id></label>
#     <label>Subject Label <input name=subject_label></label>
#     <label>Session (Experiment) Label <input name=experiment_label></label>
#     <label>Scan ID <input name=scan_id></label>
#     <label>Resource Label <input name=resource_label value=MRS required></label>
#     <button type=submit>Upload</button>
#   </form>

# </body></html>
# <button type="button" onclick="openSnapshot()">Preview XNAT Snapshot</button>

# <script>
# function openSnapshot() {
#   const p = document.querySelector('[name=project_id]').value.trim();
#   const s = document.querySelector('[name=subject_label]').value.trim();
#   const e = document.querySelector('[name=experiment_label]').value.trim();
#   if (!p || !s || !e) {
#     alert("Please fill in project, subject, and experiment first.");
#     return;
#   }
#   const q = new URLSearchParams({project: p, subject: s, experiment: e});
#   window.open('/preview_screenshot?' + q.toString(), '_blank');
# }
# </script>

# """

# UPLOAD_HTML = """
# <!doctype html><html lang=en><head><meta charset=utf-8>
# <title>Raw Spectroscopy Data XNAT Uploader</title>
# <style>
#  body{font-family:sans-serif;max-width:42rem;margin:2rem auto}
#  label{display:block;margin-top:.6rem}
#  input[type=text],input[type=file]{width:100%;padding:.45rem}
#  button{margin-top:1.2rem;padding:.6rem 1.2rem;font-size:1rem}
#  .flash{background:#eef;border:1px solid #ccd;padding:.8rem;margin-top:1rem}
# </style>
# <script>
# function sanitize(txt){return txt.trim().replace(/\\W+/g,'_').replace(/^_|_$/g,'');}
# function parseRDA(text){
#   const start='>>> Begin of header <<<'; const end='>>> End of header <<<';
#   const a=text.indexOf(start), b=text.indexOf(end);
#   if(a===-1||b===-1) return {};
#   const lines = text.slice(a+start.length, b).split(/\\r?\\n/);
#   const dict = {};
#   for(const ln of lines){
#     const i = ln.indexOf(':');
#     if(i>0) dict[ln.slice(0,i).trim()] = ln.slice(i+1).trim();
#   }
#   return dict;
# }
# function applyHeader(h){
#   if(h.StudyDescription) document.querySelector('[name=project_id]').value = sanitize(h.StudyDescription);
#   if(h.PatientName)      document.querySelector('[name=subject_label]').value = sanitize(h.PatientName);
#   if(h.PatientID)        document.querySelector('[name=experiment_label]').value = sanitize(h.PatientID);
#   if(h.SeriesNumber)     document.querySelector('[name=scan_id]').value = h.SeriesNumber.trim();
# }
# function handleFiles(files){
#   for(const f of files){
#     if(f.name.toLowerCase().endsWith('.rda')){
#       const reader = new FileReader();
#       reader.onload = ev => applyHeader(parseRDA(ev.target.result));
#       reader.readAsText(f);
#       break;
#     }
#   }
# }
# window.addEventListener('DOMContentLoaded', () => {
#   document.getElementById('rda_input').addEventListener('change', e => handleFiles(e.target.files));
# });
# </script>
# </head><body>
#   <p style="float:right"><a href="{{url_for('logout')}}">Logout</a></p>
#   <h2>Upload .rda and .dat files to XNAT</h2>
#   {% with messages=get_flashed_messages() %}{% for msg in messages %}<div class=flash>{{msg|safe}}</div>{% endfor %}{% endwith %}
#   <form method=post action="{{url_for('upload')}}" enctype=multipart/form-data>
#     <label>Select .rda or .dat file(s) <input id="rda_input" name=files type=file accept=.rda,.dat multiple required></label>
#     <label>Project ID <input name=project_id></label>
#     <label>Subject Label <input name=subject_label></label>
#     <label>Session (Experiment) Label <input name=experiment_label></label>
#     <label>Scan ID <input name=scan_id></label>
#     <label>Resource Label <input name=resource_label value=MRS required></label>
#     <button type=submit>Upload</button>
#   </form>
# </body>

# <button type="button" id="xnat_preview_btn">Preview on XNAT</button>
# <iframe id="xnat_preview_frame"
#         style="width:100%;height:600px;border:1px solid #ccc;margin-top:1rem">
# </iframe>

# <script>
# // when clicked, assemble the query‑string and point the iframe at /preview
# document.getElementById('xnat_preview_btn').addEventListener('click', () => {
#   const p  = document.querySelector('[name=project_id]').value.trim();
#   const s  = document.querySelector('[name=subject_label]').value.trim();
#   const e  = document.querySelector('[name=experiment_label]').value.trim();
#   if (!p||!s||!e) {
#     return alert('Please fill Project, Subject and Session first.');
#   }
#   const q = new URLSearchParams({project:p,subject:s,experiment:e});
#   document.getElementById('xnat_preview_frame')
#           .src = `/preview?${q.toString()}`;
# });
# </script>


# </html>
# """

# ---------------------------------------------------------------------------
# Backend helpers
# ---------------------------------------------------------------------------
from flask import Flask, session, request, redirect, url_for, send_file, flash
from playwright.sync_api import sync_playwright
import os

@app.route("/preview_screenshot")
def preview_screenshot():
    if "xnat_user" not in session or "xnat_pass" not in session:
        flash("Please log in to XNAT first.")
        return redirect(url_for("login"))

    # Pull IDs from query string
    proj = request.args.get("project")
    subj = request.args.get("subject")
    exp  = request.args.get("experiment")
    if not proj or not subj or not exp:
        flash("Missing project, subject, or experiment.")
        return redirect(url_for("index"))

    # Construct the XNAT URL for that experiment
    target_url = f"{XNAT_BASE_URL}/data/projects/{proj}/subjects/{subj}/experiments/{exp}"

    # Path to store screenshot
    safe_id = f"{proj}_{subj}_{exp}".replace("/", "_")
    out_path = f"/tmp/xnat_preview_{safe_id}.png"

    try:
        generate_xnat_screenshot(target_url, session["xnat_user"], session["xnat_pass"], out_path)
    except Exception as e:
        flash(f"Screenshot failed: {e}")
        return redirect(url_for("index"))

    if not os.path.exists(out_path):
        flash("Screenshot file was not created.")
        return redirect(url_for("index"))

    return send_file(out_path, mimetype="image/png")

def generate_xnat_screenshot(xnat_url: str, user: str, password: str, out_path: str):
    """Log into Brown XNAT using Brown LDAP and capture a screenshot."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)  # use headless=True for production
        context = browser.new_context()
        page = context.new_page()

        # 1. Go to login page
        login_url = f"{XNAT_BASE_URL}/app/template/Login.vm"
        page.goto(login_url, wait_until="domcontentloaded")

        # 2. Wait briefly for JS to run
        page.wait_for_timeout(1000)

        # 3. Force the hidden dropdown to appear
        page.eval_on_selector("select[name='login_method']", "el => el.style.display = 'block'")
        page.select_option("select[name='login_method']", value="brownldap")

        # 4. Wait for user/pass fields and fill
        page.wait_for_selector("input[name='username']", timeout=10000)
        page.fill("input[name='username']", user)
        page.fill("input[name='password']", password)

        # 5. Submit form by pressing Enter on password field
        page.press("input[name='password']", "Enter")

        # 6. Wait for login to complete
        page.wait_for_load_state("networkidle")

        # 7. Go to the target page
        page.goto(xnat_url, wait_until="networkidle")

        # 8. Screenshot
        page.screenshot(path=out_path, full_page=True)

        browser.close()



def _basic_auth_header(user: str, pwd: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _session_creds() -> Tuple[str, str]:
    if "xnat_user" not in session or "xnat_pass" not in session:
        raise RuntimeError("Not logged in")
    return session["xnat_user"], session["xnat_pass"]


def _check_credentials(user: str, pwd: str) -> bool:
    url = f"{XNAT_BASE_URL}/data/projects?limit=1"
    return 200 <= _request('GET', url, None, auth=(user, pwd)) < 300


def _request(method: str, url: str, data: bytes | None, auth: Tuple[str, str] | None = None,
             timeout: int = 15) -> int:
    user, pwd = auth if auth else _session_creds()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": _basic_auth_header(user, pwd),
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code


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

def _norm_series_key(txt: str) -> str:
    """
    Return a normalised key where underscores, dashes and case
    differences no longer matter, e.g.
        'mrs-mrsref_acq-PRESS_voi-Lacc'  ->
        'mrsmrsrefacqpressvoilacc'
    """
    return re.sub(r'[_-]', '', txt).lower()

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
from urllib.parse import quote

import re
# ---------------------------------------------------------------------------
# Proxy individual assets (CSS, JS, images) from XNAT
# ---------------------------------------------------------------------------
@app.route('/preview_proxy')
def preview_proxy():
    if 'xnat_user' not in session:
        return redirect(url_for('login'))

    # path is the absolute path on XNAT (e.g. "/app/css/style.css")
    path = request.args.get('path')
    if not path or not path.startswith('/'):
        return abort(400, "Missing or invalid 'path' parameter")

    # build full URL
    url = f"{XNAT_BASE_URL.rstrip('/')}{path}"
    req = urllib.request.Request(url, headers={
        "Authorization": _basic_auth_header(*_session_creds()),
        "Accept": "*/*"
    })
    try:
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            ctype = resp.headers.get("Content-Type", "application/octet-stream")
        return Response(data, content_type=ctype)
    except urllib.error.HTTPError as e:
        # forward error code & body
        err_body = e.read()
        err_ctype = e.headers.get("Content-Type", "text/plain")
        return Response(err_body, status=e.code, content_type=err_ctype)


# ---------------------------------------------------------------------------
# Preview route, now rewriting all src/href to use preview_proxy
# ---------------------------------------------------------------------------
from urllib.parse import quote as urlquote


@app.route('/preview_inline')
def preview_inline():
    if 'xnat_user' not in session:
        return redirect(url_for('login'))

    proj = request.args.get('project')
    subj = request.args.get('subject')
    exp  = request.args.get('experiment')
    if not proj or not subj or not exp:
        flash("Need project, subject, and experiment to preview")
        return redirect(url_for('index'))

    # fetch the HTML from XNAT
    url = f"{XNAT_BASE_URL}/data/projects/{proj}/subjects/{subj}/experiments/{exp}"
    req = urllib.request.Request(url, headers={
        "Authorization": _basic_auth_header(*_session_creds()),
        "Accept": "text/html"
    })

    with urllib.request.urlopen(req) as resp:
        html = resp.read().decode('utf-8', errors='replace')

    # clean it, strip scripts, inject <base>
    html = re.sub(r'<script[^<]+</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<script[^>]*>', '', html, flags=re.IGNORECASE)
    base_tag = f'<base href="{XNAT_BASE_URL.rstrip("/")}/">'
    html = re.sub(r'(<head[^>]*>)', r'\1\n    ' + base_tag, html, count=1, flags=re.IGNORECASE)

    # minimal rewrite of src/href
    html = re.sub(
        r'(?P<attr>src|href)=(?P<quote>["\'])(?P<path>/[^"\']+)(?P=quote)',
        lambda m: (
            f'{m.group("attr")}={m.group("quote")}'
            f'{XNAT_BASE_URL}{m.group("path")}'
            f'{m.group("quote")}'
        ),
        html,
        flags=re.IGNORECASE
    )

    # render within a template instead of iframe
    return render_template_string("""
    <h2>XNAT Snapshot: {{ proj }}/{{ subj }}/{{ exp }}</h2>
    <div class="xnat-preview" style="border:1px solid #ccc;padding:1rem">
      {{ snapshot_html|safe }}
    </div>
    """, proj=proj, subj=subj, exp=exp, snapshot_html=html)




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

@app.route('/', methods=['GET'])
def index():
    if 'xnat_user' not in session:
        return redirect(url_for('login'))
    return render_template_string(UPLOAD_HTML)

@app.route('/upload', methods=['POST'])
def upload():
    if "xnat_user" not in session:
        return redirect(url_for("login"))

    rp    = request.form
    p0,s0,e0,sc0, d0 = (
        rp.get("project_id")       or None,
        rp.get("subject_label")    or None,
        rp.get("experiment_label") or None,
        rp.get("scan_id")          or None,
        rp.get("study_date")          or None
    )
    label = rp["resource_label"].strip()

    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        flash("No files selected."); return redirect(url_for("index"))

    ok, bad = 0, []
    # 1) First, parse all RDAs, build a map seriesDesc -> (p,s,e,sc)
    rda_map: Dict[str, Tuple[str,str,str,str]] = {}
    for f in files:
        if not f.filename.lower().endswith(".rda"):
            continue

        # read the raw bytes, parse, then rewind
        blob = f.stream.read()
        p, s, e, sc, d, _ = _derive_ids(blob, f.filename, p0, s0, e0, sc0, d0)
        hdr = _parse_hdr(blob)
        sd = _norm_series_key(hdr.get("SeriesDescription"))
        if sd and all([p, s, e, sc is not None]):
            rda_map[sd.strip()] = (p, s, e, sc, d)

        # rewind so the upload loop can read the full file
        try:
            f.stream.seek(0)
        except AttributeError:
            # if FileStorage proxies .seek, you can also try f.seek(0)
            f.seek(0)


    # 2) Now handle uploads, using RDA info for both RDAs and DATs
    for f in files:
        name = f.filename
        ext  = name.lower().rsplit(".",1)[-1]
        blob = f.read() if ext == "rda" else f.read()  # just read once
        if ext == "rda":
            # upload RDA exactly as before
            p,s,e,sc,d, msgs = _derive_ids(blob, name, p0,s0,e0,sc0, d0)
        elif ext == "dat":
            # extract seriesDesc from filename
            m = re.match(r"^meas_[^_]+_[^_]+_(.+)\.dat$", name, re.IGNORECASE)
            if not m:
                bad.append(f"{name}: invalid DAT filename format")
                continue
            sd = _norm_series_key(m.group(1))
            if sd not in rda_map:
                bad.append(f"{name}: no matching RDA for series '{sd}'")
                continue
            p,s,e,sc,d = rda_map[sd]
            msgs = [f"{name}: matched to RDA series '{sd}'"]
        else:
            bad.append(f"{name}: unsupported extension")
            continue

        # # flash any messages
        # for m in msgs:
        #     flash(m)

        # now do the upload
        base = f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
        if sc:
            base += f"/scans/{sc}"
        _ensure_resource(base, label)

        # before uploading, check DICOM metadata on XNAT to be sure it is spectroscopy
        # and date matches
        scan = f"/archive/projects/{p}/subjects/{s}/experiments/{e}/scans/{sc}"
        img_type = dicom_field(XNAT_BASE_URL,scan, "ImageType")     # ORIGINAL\PRIMARY\SPECTROSCOPY\NONE
        study_dt = dicom_field(XNAT_BASE_URL,scan, "StudyDate")     # 20250724
        series_description = dicom_field(XNAT_BASE_URL,scan, "SeriesDescription")
        expected_dt = d                      # derive as needed

        if "SPECTROSCOPY" not in img_type.upper():
            bad.append(f"Destination DICOM {series_description} is not spectroscopy (ImageType='{img_type}'). Not uploading {name}<br>")
            continue
            # raise ValueError(f"{scan}: not spectroscopy (ImageType='{img_type}')")
        elif study_dt != expected_dt:
            bad.append(f"{scan}: StudyDate {study_dt} ≠ {expected_dt}")
            continue
            # raise ValueError(f"{scan}: StudyDate {study_dt} ≠ {expected_dt}")
        else:
            msgs = ["header checks passed — ready to upload .rda"]

            endpoint = (
                f"{base}/resources/{label}/files/"
                f"{urllib.parse.quote(name)}?inbody=true"
            )
            status = _request("PUT", endpoint, blob)
            if 200 <= status < 300:
                ok += 1
            else:
                bad.append(f"{name}: HTTP {status}")

            # flash any messages
    for m in msgs:
        flash(m)

    # flash summary
    if ok:
        flash(f"<strong>{ok}</strong> file(s) uploaded")
    if bad:
        flash("<br>".join(bad))

    return redirect(url_for("index"))

# ---------------------------------------------------------------------------
# Tests (offline)
# ---------------------------------------------------------------------------
class _HDRTests(unittest.TestCase):
    SAMPLE = (
        b">>> Begin of header <<<\nStudyDescription: DEMO\nPatientName: Foo Bar\nPatientID: P1\nSeriesNumber: 2\n>>> End of header <<<rest"
    )
    def test_parse(self):
        h = _parse_hdr(self.SAMPLE)
        self.assertEqual(h['PatientName'], 'Foo Bar')
    def test_derive(self):
        p,s,e,sc,_ = _derive_ids(self.SAMPLE, 'x.rda', None, None, None, None)
        self.assertEqual(p, 'DEMO')
        self.assertEqual(s, 'Foo_Bar')
        self.assertEqual(e, 'P1')
        self.assertEqual(sc, '2')

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
