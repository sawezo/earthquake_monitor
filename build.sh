#!/bin/sh


# preliminary
chmod -R 777 ./backend/airflow/logs/


# run container
docker-compose up -d --force-recreate


# setup mongo users
sleep 10
docker exec mongo_db mongo admin ./setup/create-admin.js
sleep 5
docker exec mongo_db mongo admin ./setup/create-user.js -u $MONGO_ADMIN_USER -p $MONGO_ADMIN_PASSWORD --authenticationDatabase admin


# starting the pyflink stream job
sleep 10
docker exec jobmanager ./bin/flink run -py /opt/jobs/pipeline.py


# starting the pyflink stream job
sleep 5
docker exec flink_job_manager ./bin/flink run -py /opt/jobs/pipeline.py


# setup airflow  
# sleep 30
# docker exec airflow_webserver airflow webserver
# docker exec airflow_webserver airflow db init
# docker exec airflow_webserver airflow users create --role Admin --username admin --password admin --email admin --firstname admin --lastname admin

# sleep 5
# docker exec airflow_scheduler dags unpause 'train_model'