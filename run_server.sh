#!/bin/bash

python3 -m server.server --init

pytest -s tests/test_server.py

