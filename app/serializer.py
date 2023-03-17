from typing import Any, Iterator

class RESPSerializer:
    encoding: str

    def __init__(self, encoding: str):
        self.encoding = encoding

    def deserialize(self, data: bytes) -> Iterator[str]:
        decoded = data.decode(self.encoding).split()
        for s in decoded:
            yield s

    def serialize(self, obj: Any) -> bytes:
        if isinstance(obj,str):
            return self._encode_string(obj)
        return b''

    def _encode_string(self, string: str) -> bytes:
        return bytes(f"+{string}\r\n", self.encoding)