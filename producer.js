const { default: axios } = require('axios');
const kafka = require('kafka-node');

const kafkaHost = 'localhost:9092';
const producer = new kafka.Producer(new kafka.KafkaClient({ 
    kafkaHost,
    batch: {
        size: 2000000,  // 2 MB
        linger: 1000, // 20000 milliseconds (20 second)
    }
 }));

// const producer = new kafka.Producer({
//     kafkaHost: 'localhost:9092',
//     batch: {
//         size: 2000000,  // 2 MB
//         linger: 1000, // 20000 milliseconds (20 second)
//     },
// });

const apiRequestTopic = 'test';

producer.on('ready', async () => {
    console.log('Kafka Producer is ready');

    // Sample API request
    const axiosConfig = {
        method: "post",
        url: `https://api.logicalcrm.com/api/auth`,
        data: {
            "username": "sameer",
            "password": "inter@bit"
        },
        headers: {
            "Content-Type": "application/json",
        },
    };

    // Make the actual REST API call here
    // const apiResponse = await makeApiCall(axiosConfig);

    let apiResponse = { "name": "sameer", "age": "31"};
    // Produce the API request to the Kafka topic
    const payloads = [
        { topic: apiRequestTopic, messages: JSON.stringify(apiResponse) }
    ];

    producer.send(payloads, (err, data) => {
        if (err) {
            console.error('Error producing API request:', err);
        } else {
            console.log('API request produced successfully:', data);
        }
    });
});

producer.on('error', (err) => {
    console.error('Error initializing Kafka Producer:', err);
});

const makeApiCall = async (apiRequest) => {
    // Implement your API call logic here
    // For simplicity, let's assume a synchronous function that returns a response.
    try {
        const res = await axios(apiRequest);
        return { status: res.status, data: res.data };
    }
    catch (error) {
        console.log("🚀 ~ makeApiCall ~ error:", error.message);
        return { status: 500, message: error.message };
    }
};