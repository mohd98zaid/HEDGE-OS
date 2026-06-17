const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8088');

ws.on('open', () => {
    console.log('Connected to Vite proxy');
    ws.send(JSON.stringify({
        type: 'subscribe',
        channel: 'market'
    }));
});

ws.on('message', (data) => {
    console.log('Received:', data.toString());
});

ws.on('error', (err) => {
    console.error('Error:', err);
});

ws.on('close', () => {
    console.log('Closed');
});
