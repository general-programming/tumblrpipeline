const http = require('http');

const {promisify} = require('util');
const client = require('prom-client');
const redis = require('redis');

// Constants
const QUEUE_KEYS = [,
  "tumblr:queue:posts",
  "tumblr:queue:blogs",
  "tumblr:queue:import",
  "tumblr:queue:import:working",
  "tumblr:queue:manualqueue"
];

// Server setup
const register = client.register;
const port = 3000;

const requestHandler = (request, response) => {
  response.end(register.metrics())
}

const server = http.createServer(requestHandler)
const redis_client = redis.createClient();
const redis_hgetall = promisify(redis_client.hgetall).bind(redis_client);
const redis_scard = promisify(redis_client.scard).bind(redis_client);

redis_client.on("error", function (err) {
    console.log("Error " + err);
});

server.listen(port, (err) => {
  if (err) {
    return console.log('something bad happened', err)
  }

  console.log(`server is listening on ${port}`)
})

// Gauges
const workerPostsGauge = new client.Gauge({
  name: 'worker_posts',
  help: 'posts sent by each worker',
  labelNames: ['worker']
});

const queueSizeGauge = new client.Gauge({
  name: 'queue_size',
  help: 'size of each internal queue',
  labelNames: ['queue']
});

// Gauge updater
const updateGauges = async () => {
  let work_done = await redis_hgetall("tumblr:work_stats");
  Object.keys(work_done).forEach((worker) => {
    let work = parseInt(work_done[worker]);
    workerPostsGauge.set({worker}, work);
  });

  for (let i in QUEUE_KEYS) {
    let queue = QUEUE_KEYS[i];
    let queueSize = await redis_scard(queue);
    queueSizeGauge.set({queue}, queueSize);
  };
};

setInterval(updateGauges, 500);
