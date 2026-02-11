# XNAT MRS Uploader

Local Flask web app for bulk uploading Siemens MRS spectroscopy files (`.rda` and `.dat`) into XNAT scan resources.

This app is intended to be run **locally** (default bind: `127.0.0.1`) and accessed in a web browser.

Repo: [https://github.com/brown-bnc/XNAT-uploader-webapp](https://github.com/brown-bnc/XNAT-uploader-webapp)

---

## Install (Python venv)

> These instructions install from a **tagged release**, which is recommended for reproducible installs.

### macOS / Linux

```bash
python3 -m venv .xnat_uploader_venv
source .xnat_uploader_venv/bin/activate
python -m pip install --upgrade pip

pip install https://github.com/brown-bnc/XNAT-uploader-webapp/archive/refs/tags/v0.1.1.zip

xnat-mrs-uploader
```

### Windows (PowerShell)

```powershell
py -m venv .xnat_uploader_venv
.\.xnat_uploader_venv\Scripts\Activate.ps1
python -m pip install --upgrade pip

pip install https://github.com/brown-bnc/XNAT-uploader-webapp/archive/refs/tags/v0.1.1.zip

xnat-mrs-uploader
```
