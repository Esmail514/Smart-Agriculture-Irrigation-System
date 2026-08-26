import socket
import json


DOCKER_SOCK = "/var/run/docker.sock"


def docker_exec(container: str, cmd: list[str], timeout: int = 600) -> str:
    """
    Execute a command inside a Docker container via the Docker Engine API.
    Uses the Unix socket directly — no Docker CLI required.
    """
    body = json.dumps({
        "AttachStdout": True,
        "AttachStderr": True,
        "Tty": False,
        "Cmd": cmd,
    })

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect(DOCKER_SOCK)

    req = (
        f"POST /containers/{container}/exec HTTP/1.1\r\n"
        f"Host: localhost\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
        f"{body}"
    )
    sock.sendall(req.encode())

    resp = _recv_all(sock)
    sock.close()

    header_end = resp.find(b"\r\n\r\n")
    payload = json.loads(resp[header_end + 4:])
    exec_id = payload["Id"]

    sock2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock2.settimeout(timeout)
    sock2.connect(DOCKER_SOCK)

    start_body = json.dumps({"Detach": False, "Tty": False})
    req2 = (
        f"POST /exec/{exec_id}/start HTTP/1.1\r\n"
        f"Host: localhost\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(start_body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
        f"{start_body}"
    )
    sock2.sendall(req2.encode())

    raw = _recv_all(sock2)
    sock2.close()

    header_end2 = raw.find(b"\r\n\r\n")
    stream = raw[header_end2 + 4:]

    output = b""
    pos = 0
    while pos + 8 <= len(stream):
        size = int.from_bytes(stream[pos + 4 : pos + 8], "big")
        pos += 8
        output += stream[pos : pos + size]
        pos += size

    return output.decode(errors="replace")


def _recv_all(sock: socket.socket) -> bytes:
    chunks = []
    while True:
        try:
            data = sock.recv(4096)
            if not data:
                break
            chunks.append(data)
        except socket.timeout:
            break
    return b"".join(chunks)
