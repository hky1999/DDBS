#!/bin/bash

docker pull redis:latest
docker pull mongo:latest

pip3 install -r requirements.txt

# docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.2.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
