# -*- coding: utf-8 -*-
"""
Download PDFs from goszakup v3bl links to inputs/.

Usage:
  python download_pdfs.py --urls urls.txt

urls.txt example (one per line):
https://v3bl.goszakup.gov.kz/files/download_file/295321418/
"""
from pathlib import Path
import argparse
import requests

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--urls", required=True, help="Text file with URLs, one per line")
    ap.add_argument("--outdir", default="inputs", help="Where to save PDFs (default: inputs/)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    urls = [u.strip() for u in Path(args.urls).read_text(encoding="utf-8").splitlines() if u.strip()]
    for url in urls:
        # Use last numeric segment as id
        file_id = url.rstrip("/").split("/")[-1]
        out = outdir / f"file_{file_id}.pdf"
        print(f"Downloading {url} -> {out}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        out.write_bytes(r.content)
    print("[OK] Done")

if __name__ == "__main__":
    main()
