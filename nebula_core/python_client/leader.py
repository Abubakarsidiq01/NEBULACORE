from nebula_client import NebulaClient
import sys

host = sys.argv[1]
port = int(sys.argv[2])

c = NebulaClient(host, port)
res = c.leader()
print(res)
