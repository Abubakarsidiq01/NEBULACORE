# python_client/publish.py

import sys
from nebula_client import NebulaClient


def main():
    if len(sys.argv) < 6:
        print("Usage: python publish.py <host> <port> <topic> <key> <message>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    topic = sys.argv[3]
    key = sys.argv[4]
    message = sys.argv[5]

    client = NebulaClient(host, port)
    res = client.publish(topic, key, message)
    print(res)


if __name__ == "__main__":
    main()
