#!/usr/bin/env python3
# encoding: utf-8

import pytest

from server.server import DDBS


@pytest.fixture
def client():
    DDBS.config["TESTING"] = True
    with DDBS.app_context():
        yield DDBS.test_client()


def test_server_status(client):
    resp = client.get("/status", content_type="html/text")


def test_response(client):
    resp = client.get("/", content_type="html/text")
    assert resp.data == b"DDBS"


def test_get_user(client):
    resp = client.get("/users")
    resp = client.get("/user/4")  # cold
    resp = client.get("/user/4")


def test_get_user_read(client):
    resp = client.get("/user_read/3")
    # print(resp.data)


def test_get_popular_rank(client):
    resp = client.get("/popular")
    # print(resp.data)
