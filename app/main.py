import socket


def main():

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    print(f"Connected to {addr}")
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                continue
            resp = data.decode('utf-8').split()
            if len(resp) > 0:
                print(f"received {resp}")
            for s in resp:
                if s.lower() == "ping":
                    conn.sendall(b'+PONG\r\n')


if __name__ == "__main__":
    main()
