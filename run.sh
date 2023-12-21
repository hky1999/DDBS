#!/bin/bash

# Machine 0
docker run -d --rm -p 20000:6379 --name cache0 redis
docker run -d --rm -p 20001:27017 --name dbms0 mongo

# Machine 1
docker run -d --rm -p 20002:6379 --name cache1 redis
docker run -d --rm -p 20003:27017 --name dbms1 mongo

# Hadoop FS
cd docker-hadoop
docker-compose up -d
# docker-compose down
cd ..

# Show running containers
docker ps -a
