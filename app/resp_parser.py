from typing import Callable, Dict, List, Optional, Tuple, Union

ParsedRESP = Union[Optional[str],int,'NestedArr']
NestedArr = List[ParsedRESP] # arbitrarily nested array

class RESPParser:
    _handlers: Dict[str,Callable[[str,int,int],Tuple[ParsedRESP,int]]]

    def __init__(self):
        self._handlers = {
            "+": self._parse_simple_string,
            "$": self._parse_bulk_string,
            "*": self._parse_array,
            ":": self._parse_integer,
        }

    def parse(self, input: str) -> ParsedRESP:
        r = input.find('\r')
        if r == -1:
            raise ValueError("Input must be formatted according to RESP protocol")
        val,_ = self._handlers[input[0]](input,0,r)
        return val

    @staticmethod
    def _parse_simple_string(input: str, l: int, r:int) -> Tuple[str,int]:
        return input[l+1:r],0

    @staticmethod
    def _parse_bulk_string(input: str, l: int, r:int) -> Tuple[str,int]:
        skip = int(input[l+1:r])
        if skip == -1:
            return None,0
        return input[r+2:r+2+skip],skip+2

    @staticmethod
    def _parse_integer(input: str, l: int, r:int) -> Tuple[int,int]:
        return int(input[l+1:r]),0

    def _parse_array(self, input: str, l: int, r: int) -> Tuple[NestedArr,int]:
        retval = []
        rem = int(input[l+1:r])
        if rem == -1:
            return [None]
        total_skip = 0
        # update ptrs to skip '\r\n'
        l = r+2
        r += 2
        while r < len(input) and rem > 0:
            if input[r] != '\r':
                r += 1
                continue
            val,skip = self._handlers[input[l]](input,l,r)
            retval.append(val)
            total_skip += skip
            # update ptrs to skip '\r\n' + any already-parsed tokens
            r += 2 + skip
            l = r
            rem -= 1

        return retval,total_skip
        
