import socket
import struct


class NebulaClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    # low-level frame: [len:u32][payload...]
    def _send_frame(self, host: str, port: int, payload: bytes) -> bytes | None:
        frame = struct.pack(">I", len(payload)) + payload
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
            s.sendall(frame)

            header = s.recv(4)
            if not header:
                return None

            (length,) = struct.unpack(">I", header)

            body = b""
            while len(body) < length:
                chunk = s.recv(length - len(body))
                if not chunk:
                    break
                body += chunk

            return body
        finally:
            try:
                s.close()
            except Exception:
                pass

    def leader(self):
        payload = bytes([1])  # cmd = 1 (LEADER_INFO)
        body = self._send_frame(self.host, self.port, payload)
        if not body:
            return None

        status = body[0]
        if status != 0:
            return {"status": "error"}

        name_len = struct.unpack(">H", body[1:3])[0]
        name = body[3 : 3 + name_len].decode()
        return {"status": "ok", "leader": name}

    # cmd = 2 (PUBLISH)
    def publish(self, topic: str, key: str, message: str) -> dict:
        max_redirects = 5
        host = self.host
        port = self.port

        for _ in range(max_redirects):
            topic_b = topic.encode()
            key_b = key.encode()
            msg_b = message.encode()

            payload = bytearray()
            payload.append(2)  # cmd

            # topic
            payload += struct.pack(">H", len(topic_b))
            payload += topic_b

            # key
            payload += struct.pack(">H", len(key_b))
            payload += key_b

            # message
            payload += struct.pack(">I", len(msg_b))
            payload += msg_b

            body = self._send_frame(host, port, bytes(payload))
            if not body:
                return {"status": "error", "message": "no response from server"}

            status = body[0]

            # ok
            if status == 0:
                if len(body) < 1 + 4 + 8:
                    return {"status": "error", "message": "short ok frame"}
                partition = struct.unpack(">I", body[1:5])[0]
                offset = struct.unpack(">Q", body[5:13])[0]
                # update our current host/port for future calls
                self.host = host
                self.port = port
                return {
                    "status": "ok",
                    "partition": partition,
                    "offset": offset,
                }

            # redirect
            if status == 1:
                if len(body) < 1 + 2:
                    return {
                        "status": "error",
                        "message": "short redirect frame",
                    }
                name_len = struct.unpack(">H", body[1:3])[0]
                if len(body) < 3 + name_len + 2:
                    return {
                        "status": "error",
                        "message": "bad redirect frame",
                    }
                host_str = body[3 : 3 + name_len].decode()
                port_val = struct.unpack(">H", body[3 + name_len : 3 + name_len + 2])[0]
                host = host_str
                port = port_val
                # loop and retry with new host/port
                continue

            # error
            if status == 2:
                if len(body) < 3:
                    return {"status": "error", "message": "error frame too short"}
                msg_len = struct.unpack(">H", body[1:3])[0]
                msg = body[3 : 3 + msg_len].decode()
                return {"status": "error", "message": msg}

            return {"status": "error", "message": f"unknown status {status}"}

        return {"status": "error", "message": "too many redirects"}

    # cmd = 3 (CONSUME)
    def consume_one(self, topic: str, group: str, partition: int) -> dict:
        topic_b = topic.encode()
        group_b = group.encode()

        payload = bytearray()
        payload.append(3)  # cmd

        payload += struct.pack(">H", len(topic_b))
        payload += topic_b

        payload += struct.pack(">H", len(group_b))
        payload += group_b

        payload += struct.pack(">I", partition)

        body = self._send_frame(self.host, self.port, bytes(payload))
        if not body:
            return {"status": "error", "message": "no response from server"}

        status = body[0]

        if status == 0:
            if len(body) < 1 + 4:
                return {"status": "error", "message": "short consume ok frame"}
            msg_len = struct.unpack(">I", body[1:5])[0]
            if len(body) < 5 + msg_len:
                return {"status": "error", "message": "bad consume msg length"}
            msg = body[5 : 5 + msg_len].decode()
            return {"status": "ok", "message": msg}

        if status == 1:
            return {"status": "empty"}

        if status == 2:
            if len(body) < 3:
                return {"status": "error", "message": "consume error frame too short"}
            msg_len = struct.unpack(">H", body[1:3])[0]
            msg = body[3 : 3 + msg_len].decode()
            return {"status": "error", "message": msg}

        return {"status": "error", "message": f"unknown status {status}"}
