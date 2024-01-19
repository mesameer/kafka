// Install kafka-node: npm install kafka-node

const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);

const apiRequestTopic = 'api_requests';
const apiResponseTopic = 'api_responses';

// Node.js Kafka Producer (Producer of API Requests)
const produceApiRequest = async (apiRequest) => {
  const payloads = [{ topic: apiRequestTopic, messages: JSON.stringify(apiRequest) }];

  producer.send(payloads, (err, data) => {
    if (err) {
      console.error('Error producing API request:', err);
    } else {
      console.log('API request produced successfully:', data);
    }
  });
};

// Node.js Kafka Consumer (Consumer of API Requests)
const consumerGroup = 'api-consumer-group';
const consumerOptions = { kafkaHost: 'localhost:9092', groupId: consumerGroup, autoCommit: true };

const apiConsumer = new kafka.ConsumerGroup(consumerOptions, apiRequestTopic);
apiConsumer.on('message', async (message) => {
  const apiRequest = JSON.parse(message.value);
  
  // Make the actual REST API call here
  const apiResponse = await makeApiCall(apiRequest);

  // Produce API Response to another Kafka topic
  produceApiResponse(apiResponse);
});

// Node.js Kafka Producer (Producer of API Responses)
const produceApiResponse = (apiResponse) => {
  const payloads = [{ topic: apiResponseTopic, messages: JSON.stringify(apiResponse) }];

  producer.send(payloads, (err, data) => {
    if (err) {
      console.error('Error producing API response:', err);
    } else {
      console.log('API response produced successfully:', data);
    }
  });
};

// Node.js Kafka Consumer (Consumer of API Responses for Database Storage)
const dbConsumer = new kafka.ConsumerGroup(consumerOptions, apiResponseTopic);
dbConsumer.on('message', async (message) => {
  const apiResponse = JSON.parse(message.value);

  // Store the API response in the database
  await storeInDatabase(apiResponse);
});

// Function to make the actual REST API call
const makeApiCall = async (apiRequest) => {
  // Implement your API call logic here
  // For simplicity, let's assume a synchronous function that returns a response.
  return { requestId: apiRequest.requestId, data: 'Sameer' };
};

// Function to store API response in the database
const storeInDatabase = async (apiResponse) => {
  // Implement your database storage logic here
  console.log('Storing API response in the database:', apiResponse);
};

// Example: Produce a sample API request
const sampleApiRequest = { requestId: '123', endpoint: '/sample', method: 'GET' };
produceApiRequest(sampleApiRequest);

// Handle process termination gracefully
process.on('SIGINT', () => {
  apiConsumer.close(true, () => {
    dbConsumer.close(true, () => {
      process.exit();
    });
  });
});