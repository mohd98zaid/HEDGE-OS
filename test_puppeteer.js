const { exec, spawn } = require('child_process');
const puppeteer = require('puppeteer');
const path = require('path');

(async () => {
  console.log('Starting Synthetic Mode...');
  const syntheticProcess = spawn('cmd.exe', ['/c', 'start-synthetic.bat'], {
    cwd: path.resolve('.'),
    detached: true,
    stdio: 'ignore'
  });

  // Give the system time to start (Gateway, Demo Synth, UI)
  await new Promise(r => setTimeout(r, 10000));

  const browser = await puppeteer.launch({ headless: 'new' });
  const page = await browser.newPage();
  
  console.log('Navigating to dashboard...');
  await page.goto('http://localhost:5173', { waitUntil: 'networkidle2' });
  
  // Wait for synthetic data to arrive
  await new Promise(r => setTimeout(r, 15000));
  
  await page.screenshot({ path: 'synthetic_test.png', fullPage: true });
  console.log('Screenshot taken: synthetic_test.png');
  
  // Output HTML for debugging
  const marketHtml = await page.evaluate(() => {
    const root = document.querySelector('#root');
    return root ? root.innerHTML.substring(0, 800) : 'No #root found';
  });
  console.log('Market HTML: \n   ', marketHtml);
  
  await browser.close();
  
  console.log('Shutting down synthetic processes...');
  exec('taskkill /f /im node.exe');
  exec('taskkill /f /im hedge-ui-gateway.exe');
  exec('taskkill /f /im hedge-demo-synth.exe');
  process.exit(0);
})();
