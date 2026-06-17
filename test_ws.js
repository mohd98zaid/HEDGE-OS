const WebSocket = require('ws');
const ws = new WebSocket('ws://localhost:8088/v1/stream');

ws.on('open', function open() {
  console.log('connected');
  ws.send(JSON.stringify({ type: 'subscribe', channel: 'market' }));
});

ws.on('message', function incoming(data) {
  const msg = JSON.parse(data);
  if (msg.channel === 'market' && msg.payload && msg.payload.kind === 'tick') {
    console.log(JSON.stringify(msg));
    process.exit(0);
  }
});
