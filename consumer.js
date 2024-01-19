const kafka = require('kafka-node');

const kafkaHost = 'localhost:9092';
const apiRequestTopic = 'test';

const consumerOptions = {
    kafkaHost,
    groupId: 'api-consumer-group',
    autoCommit: true,
};

const consumer = new kafka.ConsumerGroup(consumerOptions, apiRequestTopic);

consumer.on('message', async (message) => {
    const messageData = JSON.parse(message.value);

    // Log the API response (replace with your storage logic)
    console.log('API Response:', messageData);
});

consumer.on('error', (err) => {
    console.error('Error in Kafka Consumer:', err);
});