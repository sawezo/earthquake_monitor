#!/bin/sh


# preliminary
chmod -R 777 ./backend/airflow/logs/


# run container
docker-compose up -d --force-recreate


# setup mongo users
sleep 15
docker exec mongo_db mongo admin ./setup/create-admin.js
sleep 10
docker exec mongo_db mongo admin ./setup/create-user.js -u $MONGO_ADMIN_USER -p $MONGO_ADMIN_PASSWORD --authenticationDatabase admin


# starting the pyflink stream job
# sleep 10
# docker exec flink_job_manager ./bin/flink run -py /opt/jobs/pipeline.py


# setup airflow  
# sleep 15
# docker exec webserver airflow users create --role Admin --username admin --password admin --email admin --firstname admin --lastname admin
# sleep 10
# docker exec scheduler airflow dags unpause 'train_model'