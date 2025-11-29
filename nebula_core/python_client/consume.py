import sys
import time
from nebula_client import NebulaClient


def main():
    if len(sys.argv) < 6:
        print("Usage: python consume.py <host> <port> <topic> <group> <partition>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]
    group = sys.argv[4]
    partition = int(sys.argv[5])

    client = NebulaClient(host, port)

    print(f"Consuming from {host}:{port}, topic={topic}, group={group}, partition={partition}")
    while True:
        res = client.consume_one(topic, group, partition)
        if res["status"] == "ok":
            print("message:", res["message"])
        elif res["status"] == "empty":
            time.sleep(0.5)
        else:
            print("error:", res)
            time.sleep(1)


if __name__ == "__main__":
    main()
