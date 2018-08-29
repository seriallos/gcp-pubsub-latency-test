const _ = require('lodash');
const PubSub = require('@google-cloud/pubsub');
const shortid = require('shortid');

const mode = process.argv[2];

const pubsub = new PubSub();

const JOB_TOPIC = 'gcp-pubsub-test-job';
const JOB_SUBSCRIPTION = 'gcp-pubsub-test-job-subscription';

const JOB_CREATE_INTERVAL = 1000;
const JOB_PROCESS_DURATION = JOB_CREATE_INTERVAL * 2;

const NUM_SUBSCRIBERS = 4;

const STATUS_PREFIX = 'gcp-pubsub-test-status';

const getTopic = async topicName => {
  const topic = pubsub.topic(topicName);
  const [exists] = await topic.exists();
  if (!exists) {
    await topic.create();
  }
  return topic;
};
const getSubscription = async (topic, subscriptionName, options) => {
  const subscription = topic.subscription(subscriptionName, options);
  const [exists] = await subscription.exists();
  if (!exists) {
    await subscription.create();
  }
  return subscription;
};

const delay = async (ms) => new Promise(accept => setTimeout(accept, ms));

async function main() {

  const jobTopic = await getTopic(JOB_TOPIC);

  if (mode === 'publisher') {
    console.log('PUBLISHER: Running');

    const createJob = async () => {
      const jobId = shortid.generate();

      console.log(`PUBLISHER: Creating job ${jobId}`);

      const publisher = jobTopic.publisher({
        batching: {
          maxMessages: 0,
          maxMilliseconds: 0,
        },
      });

      const messageId = await publisher.publish(Buffer.from(JSON.stringify({
        jobId,
      })));
      console.log(`PUBLISHER: Created jobId ${jobId}, publish message ID ${messageId}`);
    }

    setInterval(createJob, JOB_CREATE_INTERVAL);

  } else if (mode === 'subscriber') {
    console.log('SUBSCRIBER: Running');

    _.each(_.range(0, NUM_SUBSCRIBERS), async subId => {
      const jobSubscription = await getSubscription(jobTopic, JOB_SUBSCRIPTION, {
        flowControl: {
          maxMessages: 1,
        },
        maxConnections: 1,
      });

      jobSubscription.on('message', async message => {
        const data = JSON.parse(message.data.toString('utf8'));
        const { jobId } = data;
        const latency = new Date(message.received).getTime() - new Date(message.publishTime).getTime();
        console.log(`SUBSCRIBER ${subId + 1}: ${jobId}/${message.id}: Received job, message latency: ${latency.toLocaleString()}ms`);

        // console.log(`SUBSCRIBER ${subId + 1}: ${jobId}/${message.id}: Processing job (${JOB_PROCESS_DURATION.toLocaleString()}ms delay)`);
        await delay(JOB_PROCESS_DURATION);

        // console.log(`SUBSCRIBER ${subId + 1}: ${jobId}/${message.id}: Finished, ACKing`);
        message.ack();
      });
    });

  } else {
    console.log('Usage: node index.js [publisher|subscriber]');
    process.exit(1);
  }
}

main();
