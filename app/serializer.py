from .resp_parser import RESPParser, ParsedRESP
from typing import Any, Optional

class RESPSerializer:
    str_encoding: str

    def __init__(self, encoding: str):
        self.str_encoding = encoding
        self._parser = RESPParser()

    def deserialize(self, data: bytes) -> ParsedRESP:
        decoded = data.decode(self.str_encoding)
        return self._parser.parse(decoded)

    def serialize(self, obj: Any, bulk_str: bool = False) -> bytes:
        if isinstance(obj,str):
            return self._serialize_bulk_string(obj) if bulk_str else self._serialize_simple_string(obj)
        if obj is None:
            return self._serialize_bulk_string(obj)
        if isinstance(obj,Exception):
            return self._serialize_error(obj)
        if isinstance(obj,list):
            return self._serialize_array(obj)
        if isinstance(obj,int):
            return self._serialize_int(obj)
        return b''

    def _serialize_simple_string(self, string: str) -> bytes:
        return bytes(f"+{string}\r\n", self.str_encoding)

    def _serialize_error(self, exception: Exception) -> bytes:
        return bytes(f"-{str(exception)}\r\n", self.str_encoding)

    def _serialize_bulk_string(self, string: Optional[str]) -> bytes:
        return (
            bytes(f"${len(string)}\r\n{string}\r\n", self.str_encoding) 
            if string is not None 
            else bytes("$-1\r\n", self.str_encoding)
        )

    def _serialize_int(self, num: int) -> bytes:
        return bytes(f":{num}\r\n")

    def _serialize_array(self, arr: list) -> bytes:
        response = bytes(f"*{len(arr)}\r\n",self.str_encoding)
        for obj in arr:
            response += self.serialize(obj,bulk_str = True)
        return response
