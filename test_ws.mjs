import WebSocket from 'ws';

const ws = new WebSocket('ws://127.0.0.1:8088/ws');

ws.on('open', () => {
    console.log('Connected');
    ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'market'
    }));
    
    // Also subscribe to other channels just in case
    ws.send(JSON.stringify({ type: 'subscribe', channel: 'signals' }));
    ws.send(JSON.stringify({ type: 'subscribe', channel: 'risk' }));
    ws.send(JSON.stringify({ type: 'subscribe', channel: 'exec' }));
});

let count = 0;
ws.on('message', (data) => {
    const msg = data.toString();
    console.log('Received:', msg.substring(0, 200) + (msg.length > 200 ? '...' : ''));
    count++;
    if (count >= 10) {
        console.log('Received 10 messages, exiting');
        process.exit(0);
    }
});

ws.on('error', (err) => {
    console.error('WS Error:', err);
});

ws.on('close', () => {
    console.log('WS Closed');
});

console.log('Waiting for messages...');
// keep process alive
setInterval(() => {}, 1000);
