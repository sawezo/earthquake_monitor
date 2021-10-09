#!/bin/sh


# run container
docker-compose up -d --force-recreate


# setup mongo users
sleep 10
docker exec mongo_db mongo admin ./setup/create-admin.js
sleep 5
docker exec mongo_db mongo admin ./setup/create-user.js -u $MONGO_ADMIN_USER -p $MONGO_ADMIN_PASSWORD --authenticationDatabase admin


# starting the pyflink stream job
sleep 10
docker-compose exec jobmanager ./bin/flink run -py /opt/jobs/pipeline.py