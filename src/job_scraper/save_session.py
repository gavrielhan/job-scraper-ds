from __future__ import annotations
import os
from pathlib import Path
from playwright.sync_api import sync_playwright

STATE_PATH = os.path.abspath(os.path.join(os.getcwd(), "data", "linkedin_state.json"))


def main() -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(180000)
        try:
            page.goto("https://www.linkedin.com/login", timeout=180000)
            # Wait until the global nav appears, which indicates a logged-in session
            try:
                page.wait_for_selector("#global-nav", timeout=180000)
            except Exception:
                pass
            context.storage_state(path=STATE_PATH)
            print(f"Saved LinkedIn session to {STATE_PATH}")
        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main() 