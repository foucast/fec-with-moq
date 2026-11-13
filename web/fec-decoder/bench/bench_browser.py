"""Decoder unit timing in a headless browser (Playwright). Serve web/fec-decoder/ on localhost:8081
after `tsc`, then:  python bench/bench_browser.py [chromium|firefox]"""
import json, sys
from playwright.sync_api import sync_playwright

name = sys.argv[1] if len(sys.argv) > 1 else "chromium"
with sync_playwright() as pw:
    b = getattr(pw, name).launch(headless=True)
    pg = b.new_page()
    pg.goto("http://localhost:8081/bench/bench.html")
    pg.wait_for_function("() => window.benchResult", timeout=120_000)
    r = pg.evaluate("() => window.benchResult")
    print(json.dumps({"browser": name, "version": b.version, "userAgent": r["userAgent"]}))
    for row in r["rows"]:
        print(json.dumps(row))
    b.close()
