import puppeteer from 'puppeteer';
import fs from 'fs';

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto('http://localhost:5173');
  // Wait a moment for websocket connection and react render
  await new Promise(r => setTimeout(r, 5000));
  const html = await page.content();
  fs.writeFileSync('dom_output_utf8.html', html, 'utf8');
  await browser.close();
})();
