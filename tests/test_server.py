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
    # resp = client.get("/users")
    resp = client.get("/user/4")  # cold
    resp = client.get("/user/4")


def test_get_user_read(client):
    resp = client.get("/user_read/3")
    # print(resp.data)


def test_set_user(client):
    resp = client.post(
        "/user/12345",
        data={
            "timestamp": "1506328859039",
            "id": "u12345",
            "uid": "12345",
            "name": "user12345",
            "gender": "male",
            "email": "email12345",
            "phone": "phone12345",
            "dept": "dept1",
            "grade": "grade3",
            "language": "en",
            "region": "Beijing",
            "role": "role0",
            "preferTags": "tags23",
            "obtainedCredits": "83",
        },
    )
    resp = client.get("/user/12345")
    print(resp.data)
    resp = client.post(
        "/user/12345",
        data={
            "timestamp": "1506328859039",
            "id": "u12345",
            "uid": "12345",
            "name": "user12345",
            "gender": "male",
            "email": "email123456",
            "phone": "phone123456",
            "dept": "dept1",
            "grade": "grade3",
            "language": "en",
            "region": "Beijing",
            "role": "role0",
            "preferTags": "tags23",
            "obtainedCredits": "83",
        },
    )
    resp = client.get("/user/12345")
    print(resp.data)


def test_get_popular_rank(client):
    resp = client.get("/popular/1506432287000")
    # print(resp.data)
