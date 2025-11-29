import sys
import time
from nebula_client import NebulaClient


def main():
    if len(sys.argv) < 5:
        print("Usage: python failover_test.py <host> <port> <topic> <key>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]
    key = sys.argv[4]

    client = NebulaClient(host, port)

    i = 0
    while True:
        msg = f"failover-{i}"
        try:
            res = client.publish(topic, key, msg)
            print(i, res)
        except Exception as ex:
            print(f"[CLIENT] exception during publish: {ex}")
            time.sleep(1)
        i += 1
        time.sleep(0.2)


if __name__ == "__main__":
    main()
