#!/bin/bash

docker stop cache0
docker stop dbms0

docker stop cache1
docker stop dbms1

cd hadoop
docker-compose down --volumes
cd ..

docker ps -a