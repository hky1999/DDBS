#!/bin/bash

# python3 -m server.server --init
python -m server.server

pytest -s tests/test_server.py

