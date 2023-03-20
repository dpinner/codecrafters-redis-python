import socket
from selectors import BaseSelector, DefaultSelector, EVENT_READ, SelectorKey
from typing import Any, Dict, Optional
from .serializer import RESPSerializer

DEFAULT_BUFFER_SIZE = 1024

class RedisServer:
    _server: socket.socket
    _buffer: int
    _selector: BaseSelector
    _serializer: RESPSerializer
    _response_map: Dict[str,callable]
    _cache: dict

    address: str
    port: int

    def __init__(self, address: str, port: int, buf_size: Optional[int] = None):

        self.address = address
        self.port = port

        self._buffer = DEFAULT_BUFFER_SIZE if buf_size is None else buf_size
        self._selector = DefaultSelector()
        self._serializer = RESPSerializer('utf-8')
        self._cache = {}

        self._response_map = {
            "PING": self._ping, 
            "ECHO": self._echo, 
            "SET": self._set, 
            "GET": self._get
        }

        self._server = socket.create_server((self.address, self.port), reuse_port=True)
        self._server.setblocking(False)

    def _accept(self, *args):
        conn, addr = self._server.accept()
        print(f"Connected to socket from {addr}")
        conn.setblocking(False)
        self._selector.register(conn, EVENT_READ, self._read)

    def _read(self, conn: socket.socket, mask: int):
        data = conn.recv(self._buffer)
        if data:
            print(f"Received {repr(data)} from {conn.getpeername()}")
            self._handle_conn(conn, data)
        else:
            self._selector.unregister(conn)
            print(f"Disconnecting from socket at {conn.getpeername()}")
            conn.close()

    def _handle_conn(self, conn: socket.socket, data: bytes):
        try:
            input = self._serializer.deserialize(data)
            if isinstance(input,str) and input.upper() in self._response_map:
                conn.sendall(self._response_map[input.upper()]())
            elif isinstance(input,list) and len(input) > 0:
                cmd,args = input[0],input[1:]
                if not isinstance(cmd,str) or cmd.upper() not in self._response_map:
                    raise ValueError("Unrecognized command")
                conn.sendall(self._response_map[cmd.upper()](args))
            else:
                raise ValueError("Unrecognized command")

        except ValueError as e:
            conn.sendall(self._serializer.serialize(e))

    def _ping(self, *args):
        return self._serializer.serialize("PONG")

    def _echo(self, input: list):
        val = input[0] if len(input) == 1 else input
        return self._serializer.serialize(val or "", bulk_str = True)

    def _set(self, input: list):
        if len(input) < 2:
            raise ValueError("Usage: SET key val [EX expiry]")
        key,val = input[:2]
        if not isinstance(val,str):
            raise ValueError("Cache values must be strings")
        self._cache[key] = val
        return self._serializer.serialize("OK")

    def _get(self, input: list):
        if len(input) == 0:
            raise ValueError("Usage: GET key")
        val = self._cache.get(input[0])
        if not (val is None or isinstance(val,str)):
            raise ValueError("Cache values must be strings")
        return self._serializer.serialize(val,bulk_str=True)


    def serve(self):
        self._selector.register(self._server, EVENT_READ, self._accept)
        while True:
            events: list[tuple[SelectorKey, int]] = self._selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

