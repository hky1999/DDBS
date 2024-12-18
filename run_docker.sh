#!/bin/bash

# Machine 0
docker run -d --rm -p 20000:6379 --privileged=true --name cache0 redis
docker run -d --rm -p 20001:27017 --privileged=true --name dbms0 mongo

# Machine 1
docker run -d --rm -p 20002:6379 --privileged=true --name cache1 redis
docker run -d --rm -p 20003:27017 --privileged=true --name dbms1 mongo

# Hadoop FS (use the existing hdfs on bic instead of a new one)
# cd hadoop
# docker-compose up -d
# cd ..

# Show running containers
docker ps
