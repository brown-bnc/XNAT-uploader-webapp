# app.py — Flask web‑app for bulk uploading *.rda* resources to XNAT
# ====================================================================
"""Drag‑and‑drop Siemens `.rda` files straight into XNAT 

Quick start
~~~~~~~~~~~
```bash
python -m pip install Flask          # if not installed
export XNAT_BASE_URL=https://xnat.bnc.brown.edu  # or any XNAT
python app.py
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
from typing import Dict, List, Optional, Tuple

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
    )
except ModuleNotFoundError:  # pragma: no cover
    print("ERROR: Flask not installed →  python -m pip install Flask")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
XNAT_BASE_URL: str = os.getenv("XNAT_BASE_URL", "https://xnat.bnc.brown.edu").rstrip("/")

app = Flask(__name__)
# IMPORTANT: change in production
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-me-pls")
app.permanent_session_lifetime = timedelta(minutes=5)
# ---------------------------------------------------------------------------
# HTML templates (strings for single‑file convenience)
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
<title>XNAT Bulk RDA Uploader</title>
<style>
 body{font-family:sans-serif;max-width:42rem;margin:2rem auto}
 label{display:block;margin-top:.6rem}
 input[type=text],input[type=file]{width:100%;padding:.45rem}
 button{margin-top:1.2rem;padding:.6rem 1.2rem;font-size:1rem}
 .flash{background:#eef;border:1px solid #ccd;padding:.8rem;margin-top:1rem}
</style>
<script>
function sanitize(txt){return txt.trim().replace(/\\W+/g,'_').replace(/^_|_$/g,'');}
function parseRDA(text){
  const start='>>> Begin of header <<<'; const end='>>> End of header <<<';
  const a=text.indexOf(start); const b=text.indexOf(end);
  if(a==-1||b==-1) return {};
  const lines=text.slice(a+start.length,b).split(/\\r?\\n/);
  const dict={};
  for(const ln of lines){const i=ln.indexOf(':'); if(i>0){dict[ln.slice(0,i).trim()]=ln.slice(i+1).trim();}}
  return dict;
}
function applyHeader(h){
    if(h.StudyDescription) document.querySelector('input[name=project_id]').value = sanitize(h.StudyDescription);
    if(h.PatientName)      document.querySelector('input[name=subject_label]').value = sanitize(h.PatientName);
    if(h.PatientID)        document.querySelector('input[name=experiment_label]').value = sanitize(h.PatientID);
    if(h.SeriesNumber)     document.querySelector('input[name=scan_id]').value = h.SeriesNumber.trim();
}
function handleFiles(fileList){
    for(const f of fileList){
        if(!f.name.toLowerCase().endsWith('.rda')) continue;
        const reader = new FileReader();
        reader.onload = ev => applyHeader(parseRDA(ev.target.result));
        reader.readAsText(f);
        break; // only first .rda used for defaults
    }
}
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('rda_input').addEventListener('change', e => handleFiles(e.target.files));
});
</script>
</head><body>
  <p style="float:right"><a href="{{url_for('logout')}}">Logout</a></p>
  <h2>Upload <code>.rda</code> files to XNAT</h2>
  {% with messages=get_flashed_messages() %}{% for msg in messages %}<div class=flash>{{msg|safe}}</div>{% endfor %}{% endwith %}
  <form method=post action="{{url_for('upload')}}" enctype=multipart/form-data>
    <label>Select .rda file(s) <input id="rda_input" name=files type=file accept=.rda multiple required></label>
    <label>Project ID <input name=project_id></label>
    <label>Subject Label <input name=subject_label></label>
    <label>Session (Experiment) Label <input name=experiment_label></label>
    <label>Scan ID <input name=scan_id></label>
    <label>Resource Label <input name=resource_label value=MRS required></label>
    <button type=submit>Upload</button>
  </form></body></html>
"""


def _basic_auth_header(user: str, pwd: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pwd}".encode()).decode()


def _session_creds() -> Tuple[str, str]:
    if "xnat_user" not in session or "xnat_pass" not in session:
        raise RuntimeError("Not logged in")
    return session["xnat_user"], session["xnat_pass"]


def _check_credentials(user: str, pwd: str) -> bool:
    """Return True if XNAT accepts the creds, else False."""
    url = f"{XNAT_BASE_URL}/data/projects?limit=1"  # tiny, public-ish endpoint
    return 200 <= _request("GET", url, None, auth=(user, pwd)) < 300


def _request(method: str, url: str, data: bytes | None, auth: Tuple[str, str] | None = None,
             timeout: int = 15) -> int:
    user, pwd = auth if auth else _session_creds()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": _basic_auth_header(user, pwd),
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # type: ignore[arg-type]
            return resp.status
    except urllib.error.HTTPError as err:
        return err.code


def _ensure_resource(base: str, label: str) -> None:
    _request("PUT", f"{base}/resources/{label}", data=b"")

# ---------------------------------------------------------------------------
# RDA helpers
# ---------------------------------------------------------------------------

def _parse_hdr(raw: bytes) -> Dict[str, str]:
    s = raw.find(b">>> Begin of header <<<"); e = raw.find(b">>> End of header <<<")
    if s == -1 or e == -1:
        return {}
    txt = raw[s+24:e].decode("latin-1", errors="replace")
    out: Dict[str, str] = {}
    for line in txt.splitlines():
        if ":" in line:
            k, v = line.split(":", 1); out[k.strip()] = v.strip()
    return out

_sanitize = lambda t: re.sub(r"\\W+", "_", t.strip()).strip("_")

# ---------------------------------------------------------------------------
# ID derivation
# ---------------------------------------------------------------------------

def _derive_ids(data: bytes, fname: str, p0, s0, e0, sc0):
    msgs: List[str] = []
    p, s, e, sc = p0, s0, e0, sc0
    if fname.lower().endswith(".rda"):
        h = _parse_hdr(data)
        if p is None and (sd := h.get("StudyDescription")):
            p = _sanitize(sd := sd)
            msgs.append(f"{fname}: Project → '{p}' (StudyDescription)")
        if s is None and (pn := h.get("PatientName")):
            s = _sanitize(pn)
            msgs.append(f"{fname}: Subject → '{s}' (PatientName)")
        if e is None and (pid := h.get("PatientID")):
            e = _sanitize(pid)
            msgs.append(f"{fname}: Session → '{e}' (PatientID)")
        if sc is None and (sn := h.get("SeriesNumber")):
            sc = sn
            msgs.append(f"{fname}: ScanID → {sc} (SeriesNumber)")
    return p, s, e, sc, msgs

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = request.form["username"].strip(); pwd = request.form["password"]
        if _check_credentials(user, pwd):
            session.permanent = True
            session["xnat_user"], session["xnat_pass"] = user, pwd
            return redirect(url_for("index"))
        flash("Invalid username or password")
    return render_template_string(LOGIN_HTML)


@app.route("/logout")
def logout():
    session.clear(); return redirect(url_for("login"))


@app.route("/", methods=["GET"])
def index():
    if "xnat_user" not in session:
        return redirect(url_for("login"))
    return render_template_string(UPLOAD_HTML)


@app.route("/upload", methods=["POST"])
def upload():
    if "xnat_user" not in session:
        return redirect(url_for("login"))

    rp = request.form
    p0, s0, e0, sc0 = (rp.get("project_id") or None, rp.get("subject_label") or None,
                       rp.get("experiment_label") or None, rp.get("scan_id") or None)
    label = rp["resource_label"].strip()

    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        flash("No files selected."); return redirect(url_for("index"))

    ok, bad = 0, []
    for f in files:
        blob = f.read()
        p, s, e, sc, msgs = _derive_ids(blob, f.filename, p0, s0, e0, sc0)
        for m in msgs: flash(m)
        if not all([p, s, e]):
            bad.append(f"{f.filename}: missing IDs"); continue
        base = f"{XNAT_BASE_URL}/data/projects/{p}/subjects/{s}/experiments/{e}"
        if sc: base += f"/scans/{sc}"
        _ensure_resource(base, label)
        endpoint = (
            f"{base}/resources/{label}/files/{urllib.parse.quote(f.filename)}?inbody=true"
        )
        status = _request("PUT", endpoint, blob)
        (ok := ok + 1) if 200 <= status < 300 else bad.append(f"{f.filename}: HTTP {status}")

    if ok: flash(f"<strong>{ok}</strong> file(s) uploaded")
    if bad: flash("<br>".join(bad))
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
        self.assertEqual(h["PatientName"], "Foo Bar")
    def test_derive(self):
        p,s,e,sc,_ = _derive_ids(self.SAMPLE, "x.rda", None,None,None,None)
        self.assertEqual(p, "DEMO"); self.assertEqual(s, "Foo_Bar"); self.assertEqual(e, "P1"); self.assertEqual(sc, "2")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--port", type=int, default=5000)
    args = ap.parse_args()
    if args.test:
        unittest.main(argv=[sys.argv[0]])
    else:
        app.run(host=args.host, port=args.port, debug=True)

if __name__ == "__main__":
    main()
