//jshint esversion:6
import kafka from 'kafka-node';
import config from './config';

/*
1. Create config for Kafka client interaction with server
2. Create kafka client
3. Create HighLevelPublisher to publish messages.
4. Create HighLevelConsumer to consumer messages
4. Create event handler when publisher is ready.
5. Once ready send messages.
6. Capture Consumer.On(message) event handler and perform operation as expected
*/

const options = {
    autoCommit: false,
    autoCommitIntervalMs: 5000,
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024,
    encoding: "utf8",
    keyEncoding: "utf8"
};

var client = new kafka.KafkaClient({
    autoConnect: true,
    clientId: 'Client001',
    connectTimeout: 10000,
    requestTimeout: 30000,
    kafkaHost: config.kafkaconfig.kafkabroker,
});
var highLevelProducer = new kafka.HighLevelProducer(client, {requireAcks: 1, ackTimeoutMs: 100, partitionerType: 2});

let payloads = [
    {topic: 'salestopic', messages: 'hi! there'},
    {topic: 'salestopic', messages: 'How are you doing today?'},
]
highLevelProducer.on('ready', () => {
    highLevelProducer.send(payloads, (err, data) => {
        if(err) {
            console.log(`Error => ${err}`);
        }
        if(data) {
            console.log(`Producer Send Callback Data => ${JSON.stringify(data)}`);
        }
    });
});

/*********** Consumer code ******************/
var consumer = new kafka.Consumer(client, [{
    topic: 'salestopic'
}], options);

consumer.on('message', function(message) {
    console.log(`Message => ${JSON.stringify(message)}`);
    consumer.commit((err, data) => {
        if(err) {
            console.log(`Consumer Commit Error => ${JSON.stringify(err)}`);
        }

        if(data) {
            console.log(`Consumer Commit Data => ${JSON.stringify(data)}`);
        }
    });
});

consumer.on('error', function(err) {
    console.log(`Consumer Error => ${err}`);
});

process.on("SIGINT", function() {
    consumer.close(true, function() {
        process.exit();
    });
});

/************************* EXIT ************************/
console.log('Press any key to exit');
process.stdin.setRawMode = true;
process.stdin.resume();
process.stdin.on('data', process.exit.bind(process, 0));