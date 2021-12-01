const kafka = require('kafka-node');
const express = require('express');
const port = 5000;
const app = express();

// Create Kafka Client and load Consumer constructer as Consumer instead of kafka.Consumer
const Consumer = kafka.Consumer,
 client = new kafka.KafkaClient({kafkaHost: 'kafka1:9093'})

// Make consumer, listening to trades_aggregated
 consumer = new Consumer(
 client, [ { topic: 'trades_aggregated', partition: 0 } ], { autoCommit: false, fromOffset: 'latest' });

// make server
const server = app.listen(port, () => {
  console.log(`Listening on port ${server.address().port}`);
});

// Allow websocket functionality
const io = require('socket.io')(server, {
  cors: {
    origin: '*',
  }
});

// Log when connection occurs
io.on('connection', client => {
  console.log('Connected', client);

  // On Kafka message, broadcast
consumer.on('message', function (message) {
    client.emit('event', message.value);
  });

// On error, log
consumer.on('error', function (message) {
    console.log(message);
});
// On Disconnect, log
client.on('disconnect', () => {
    console.log('Client disconnected');
 });
});