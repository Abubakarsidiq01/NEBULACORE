import sys
import time
from nebula_client import NebulaClient


def main():
    if len(sys.argv) < 6:
        print("Usage: python bench_publish.py <host> <port> <topic> <key> <count>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]
    key = sys.argv[4]
    count = int(sys.argv[5])

    client = NebulaClient(host, port)

    start = time.time()
    ok = 0
    errors = 0

    for i in range(count):
        msg = f"bench-{i}"
        res = client.publish(topic, key, msg)
        if res["status"] == "ok":
            ok += 1
        else:
            errors += 1
            print("error:", res)

        if (i + 1) % 100 == 0:
            print(f"sent {i+1}/{count}")

    elapsed = time.time() - start
    rate = ok / elapsed if elapsed > 0 else 0.0

    print("---- bench result ----")
    print(f"sent:    {count}")
    print(f"success: {ok}")
    print(f"errors:  {errors}")
    print(f"time:    {elapsed:.3f} s")
    print(f"rate:    {rate:.1f} msg/s")


if __name__ == "__main__":
    main()
