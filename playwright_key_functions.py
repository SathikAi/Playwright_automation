from playwright.async_api import async_playwright
import asyncio

async def playwright_key_functions_example():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        # navigate to Google
        await page.goto("https://www.google.com")

        await asyncio.sleep(5)   # keep browser open for demo
        await browser.close()

        

if __name__ == "__main__":
    asyncio.run(playwright_key_functions_example())



        # page.keyboard.press("Tab")

        # # Typing text
        # page.keyboard.type("Hello, Playwright!")

        # # Holding down a key (Shift) while typing
        # page.keyboard.down("Shift")
        # page.keyboard.type("this is uppercase")
        # page.keyboard.up("Shift")

        # # Pressing a combination of keys (Ctrl + A)
        # page.keyboard.press("Control+A")

        # browser.close()