#!/usr/bin/env python3
"""End-to-end test for airflow-auth-proxy.

This drives the proxy the way a real client would, using only HTTP Basic auth,
against a running Airflow, and checks three things:

  1. valid Basic credentials  -> 200 with a JSON body   (Basic->Bearer works)
  2. no Authorization header   -> 401 + WWW-Authenticate: Basic   (the proxy gate)
  3. wrong password            -> 401                    (auth rejected)

Check 2 is what tells "through the proxy" apart from "straight to Airflow":
Airflow answers an unauthenticated call with a JSON body and no Basic challenge,
while the proxy answers 401 with a `WWW-Authenticate: Basic` header.

It uses the Python standard library only, no third-party packages.

Configuration comes from environment variables:
  PROXY_URL   base URL of the proxy   (default http://127.0.0.1:8080)
  AF_USER     Basic username          (default airflow)
  AF_PASS     Basic password          (default airflow)
  API_PATH    Airflow API path to hit (default /api/v2/dags)
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

PROXY_URL = os.environ.get("PROXY_URL", "http://127.0.0.1:8080")
AF_USER = os.environ.get("AF_USER", "airflow")
AF_PASS = os.environ.get("AF_PASS", "airflow")
API_PATH = os.environ.get("API_PATH", "/api/v2/dags")


def get(path, username=None, password=None, timeout=10):
    """Send a GET request through the proxy.

    Returns (status_code, headers, body_bytes). A non-2xx response is returned
    the same way as a success instead of raising, so each check below can simply
    look at the status code.
    """
    url = PROXY_URL + path
    request = urllib.request.Request(url, method="GET")

    if username is not None:
        raw = username + ":" + password
        encoded = base64.b64encode(raw.encode()).decode()
        request.add_header("Authorization", "Basic " + encoded)

    try:
        response = urllib.request.urlopen(request, timeout=timeout)
        return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def wait_until_ready(timeout=90):
    """Wait until the proxy answers an HTTP request.

    The proxy has no health endpoint, so "ready" means an unauthenticated
    request gets an answer (which the proxy returns as 401).
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            status, headers, body = get(API_PATH, timeout=3)
            print("proxy is ready (unauthenticated request returned %d)" % status)
            return
        except urllib.error.URLError:
            if time.monotonic() > deadline:
                print("ERROR: proxy did not become ready at " + PROXY_URL)
                sys.exit(1)
            time.sleep(0.5)


def main():
    print("Running E2E test against " + PROXY_URL)
    wait_until_ready()

    passed = 0
    failed = 0

    # Check 1: valid Basic credentials are accepted and JSON data comes back.
    status, headers, body = get(API_PATH, AF_USER, AF_PASS)
    if status != 200:
        print("[FAIL] valid credentials should return 200, got %d" % status)
        failed += 1
    else:
        try:
            json.loads(body)
            print("[PASS] valid credentials returned 200 with a JSON body")
            passed += 1
        except json.JSONDecodeError:
            print("[FAIL] valid credentials returned 200 but the body was not JSON")
            failed += 1

    # Check 2: a request with no credentials is rejected by the proxy itself,
    # with a Basic challenge (this proves the request went through the proxy).
    status, headers, body = get(API_PATH)
    challenge = headers.get("WWW-Authenticate", "")
    if status == 401 and "Basic" in challenge:
        print("[PASS] missing credentials returned 401 with a Basic challenge")
        passed += 1
    else:
        print(
            "[FAIL] missing credentials should return 401 with a Basic "
            "challenge, got %d %r" % (status, challenge)
        )
        failed += 1

    # Check 3: a wrong password is rejected.
    status, headers, body = get(API_PATH, AF_USER, "wrong-password")
    if status == 401:
        print("[PASS] wrong password returned 401")
        passed += 1
    else:
        print("[FAIL] wrong password should return 401, got %d" % status)
        failed += 1

    print()
    print("%d passed, %d failed" % (passed, failed))
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
