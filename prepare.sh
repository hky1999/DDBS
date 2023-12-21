#!/bin/bash

docker pull redis:latest
docker pull mongo:latest

pip3 install -r requirements.txt
