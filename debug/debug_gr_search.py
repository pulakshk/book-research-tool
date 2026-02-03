from playwright.sync_api import sync_playwright

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.goodreads.com/search?q=hockey+romance")
        page.wait_for_timeout(3000)
        content = page.content()
        with open("gr_search_dump.html", "w") as f:
            f.write(content)
        browser.close()

if __name__ == "__main__":
    main()
