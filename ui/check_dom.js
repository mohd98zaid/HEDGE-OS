import puppeteer from 'puppeteer';

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto('http://localhost:5173');
  // Wait a moment for websocket connection and react render
  await new Promise(r => setTimeout(r, 5000));
  const html = await page.content();
  console.log(html);
  await browser.close();
})();
