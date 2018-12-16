# tumblr api pipeline

## Composefiles
* docker-compose.yml - Client compose file. You should be running this.
* docker-compose-server.yml - Server compose file. Handles the core for the clients.

## Running
```bash
docker-compose build --force-rm .

# After setting environs in a .env file.
docker-compose up -d
```

## Environs
Get these variables from tumblr's API console with Show Keys.
https://api.tumblr.com/console/calls/user/info
* WORKER_NAME - Defaults to anonymous.
* TUMBLR_CONSUMER_KEY - Required as a minimum.
* TUMBLR_CONSUMER_SECRET
* TUMBLR_TOKEN
* TUMBLR_TOKEN_SECRET

Set these to your central server.
* REDIS_HOST - Job managment
* [Optional] SENTRY_DSN - For bug tracking.

For servers
* POSTGRES_URL - Data storage.