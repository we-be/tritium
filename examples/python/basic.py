"""Plain SET/GET against a tritium node with redis-py.

Tritium speaks RESP, so any Redis client library works unmodified; nothing
here is tritium-specific except the port, and AUTH_PASSWORD if the node
requires one (see .env.example).

    pip install redis
    go run ./cmd/tritium              # from the repo root, in another shell
    python3 examples/python/basic.py
"""

import redis

r = redis.Redis(host="localhost", port=8080, password="change-me")

r.set("hello", b"world", ex=3600)  # every tritium key expires; EX is seconds
print(r.get("hello"))  # b"world"

# SCAN's cursor is opaque, same contract as Redis's: keep passing back what
# you're handed until it comes back 0. redis-py's helper already does that.
for key in r.scan_iter(match="hel*"):
    print(key, r.type(key))
