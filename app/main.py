import socket

BUFFER_SIZE = 1024

def parse(data):
    decoded = data.decode('utf-8').split()
    if len(decoded) > 0:
        print(f"received {decoded}")
    for s in decoded:
        if s.lower() == "ping":
            yield b'+PONG\r\n'


def handle_client(conn):
    while True:
        data = conn.recv(BUFFER_SIZE)
        if not data:
            break
        resp = parse(data)
        for r in resp:
            conn.sendall(r)


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept() # wait for client
        print(f"Connected to {addr}")
        with conn:
            handle_client(conn)


if __name__ == "__main__":
    main()
