#!/bin/bash

# setup terminal
source /spack-bic/share/spack/setup-env.sh
spack load python@3.9.0

docker pull redis:latest
docker pull mongo:latest

python -m pip install -r requirements.txt

# docker-compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.2.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
