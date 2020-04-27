var Twitter = require('twit');
const dotenv = require('dotenv');

var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    KeyedMessage = kafka.KeyedMessage,
    client = new kafka.KafkaClient(),
    producer = new Producer(client);

dotenv.config()


var client = new Twitter({
    consumer_key: process.env.consumer_key,
    consumer_secret: process.env.consumer_secret,
    access_token: process.env.access_token,
    access_token_secret: process.env.access_token_secret,
});


var stream = client.stream('statuses/filter', {
    track: 'realmadrid'       // trend you want to filter
})



producer.on('ready', function () {

    stream.on('tweet', function (tweet) {

        let message = tweet.text //tweeted text 
        producer.send([
            { topic: 'twitterNode', messages: message, partition: 0 }, // publish to topic twitterNode
            { topic: 'twitterNode', messages: message, partition: 1 },
            { topic: 'twitterNode', messages: message, partition: 2, },
        ], function (err, data) {

            if (err)
                console.log(err);

            console.log(data);
        })
    });
});








