#!/bin/bash

# Machine 0
docker run -d -p 20000:6379 --name cache0 redis
docker run -d -p 20001:27017 --name dbms0 mongo

# Machine 1
docker run -d -p 20002:6379 --name cache1 redis
docker run -d -p 20003:27017 --name dbms1 mongo
