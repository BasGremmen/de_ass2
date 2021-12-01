const kafka = require('kafka-node');
const express = require('express');
const port = 3000;
const app = express();

const Consumer = kafka.Consumer,
 client = new kafka.KafkaClient('kafka://kafka1:9092'),
 consumer = new Consumer(
 client, [ { topic: 'trades_aggregated', partition: 0 } ], { autoCommit: false });

const server = app.listen(port, () => {
  console.log(`Listening on port ${server.address().port}`);
});
const io = require('socket.io')(server, {
  cors: {
    origin: '*',
  }
});

io.on('connection', client => {
  console.log('Connected', client);

consumer.on('message', function (message) {
    client.emit('request', message.value);
  });

client.on('disconnect', () => {
    console.log('Client disconnected');
 });
});