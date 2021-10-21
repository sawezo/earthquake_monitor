# syntax=docker/dockerfile:1

# base image (using a lightweight linux distro) and directory setup
FROM python:3.9-alpine
FROM continuumio/miniconda3
LABEL Author, Samuel Zonay

# copy files over
ENV APP_HOME /front_end
WORKDIR $APP_HOME
COPY . $APP_HOME

# setting up the conda environments
RUN conda env create -f environment.yml

# make RUN commands use the new environment
SHELL ["conda", "run", "-n", "quake_front_end", "/bin/bash", "-c"]

# running the application
ENTRYPOINT ["conda", "run", "-n", "quake_front_end", "python", "-u", "app/run.py"]