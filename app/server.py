import socket
from selectors import BaseSelector, DefaultSelector, EVENT_READ, SelectorKey
from typing import Iterator, Optional
from app.serializer import RESPSerializer

DEFAULT_BUFFER_SIZE = 1024

class RedisServer:
    _server: socket.socket
    _buffer: int
    _selector: BaseSelector
    _serializer: RESPSerializer

    address: str
    port: int

    def __init__(self, address: str, port: int, buf_size: Optional[int] = None):

        self.address = address
        self.port = port

        self._buffer = DEFAULT_BUFFER_SIZE if buf_size is None else buf_size
        self._selector = DefaultSelector()
        self._serializer = RESPSerializer('utf-8')

        self._server = socket.create_server((self.address, self.port), reuse_port=True)
        self._server.setblocking(False)

    def _accept(self, *args):
        conn, addr = self._server.accept()
        print(f"Connected to {conn} from {addr}")
        conn.setblocking(False)
        self._selector.register(conn, EVENT_READ, self._read)

    def _read(self, conn: socket.socket, mask: int):
        data = conn.recv(self._buffer)
        if data:
            print(f"Received {repr(data)} from {conn}")
            self._handle_conn(conn, data)
        else:
            self._selector.unregister(conn)
            conn.close()
            print(f"Disconnected from {conn}")

    def _respond(self, conn: socket.socket, recv: str):
        if recv.upper() == "PING":
            conn.sendall(self._serializer.serialize("PONG"))

    def _handle_conn(self, conn: socket.socket, data: bytes):
        recvd: Iterator[str] = self._serializer.deserialize(data)
        for r in recvd:
            self._respond(conn,r)

    def serve(self):
        self._selector.register(self._server, EVENT_READ, self._accept)
        while True:
            events: list[tuple[SelectorKey, int]] = self._selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

