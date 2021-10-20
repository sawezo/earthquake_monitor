#!/bin/sh


# docker
docker kill $(docker ps -q)
docker rm $(docker ps -a -q)
docker rmi $(docker images -q)
docker-compose down --rmi all
docker system prune -a
docker volume prune


# module
sudo rm -r ./backend/mongo/db
mkdir ./backend/mongo/db

sudo rm -r ./backend/postgres/db
mkdir ./backend/postgres/db

# sudo rm -r ./backend/airflow/logs/
# mkdir ./backend/airflow/logs/