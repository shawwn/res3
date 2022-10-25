import io

readtable = {}
import sys
import types
import re
from typing import *
import io

import dataclasses

@dataclasses.dataclass
class Reader:
    stream: io.BufferedReader
    more: Any = None
    push: Callable[[Tuple], None] = None

class ReaderError(Exception):
    pass

class EndOfInput(ReaderError):
    pass

def defreader(ch):
    def inner(f):
        readtable[ch] = f
        return f
    return inner

def err(s: Reader, msg: str, *args, cls: Type[ReaderError] = ReaderError):
    exn = cls(msg, *args)
    # s.stream.close()
    raise exn

def peekb(s: Reader, size: int = 1):
    if val := s.stream.peek(size):
        if len(val) >= size:
            return val[:size]

def readb(s: Reader, size: int = 1):
    if val := peekb(s, size):
        assert val == (val2 := s.stream.read(size))
        return val

def expected(s: Reader, label: str):
    if s.more:
        return s.more
    else:
        return err(s, f"Expected {label}", cls=EndOfInput)

class Unexpected(ReaderError):
    pass

def unexpected(s: Reader, label: str, value):
    return err(s, f"Unexpected {label}: {value}", cls=Unexpected)

def expect(s: Reader, label: str, value: bytes):
    if val := readb(s, len(value)):
        if val == value:
            return val
        return unexpected(s, label, val)
    else:
        return expected(s, label)

def read_crlf(s: Reader):
    return expect(s, "<CR><LF>", b"\r\n")

def read_sign_or_digit(s: Reader):
    b = peekb(s)
    if not b:
        return expected(s, "sign or digit")
    if not b.isdigit() and b not in [b"+", b"-"]:
        return unexpected(s, "sign or digit", b)
    return readb(s)

def read_until(s: Reader, label: str, until: bytes, maxlen=None):
    bs = []
    while True:
        if not (b := peekb(s)):
            return expected(s, f"any of {until}")
        if b in until:
            break
        bs.append(readb(s))
        if maxlen is not None and len(bs) >= maxlen:
            return err(s, "overflow in " + label)
    return b''.join(bs)

def read_int64(s: Reader, signed: bool = True, label: str = None, until: bytes = b"\r\n"):
    if label is None:
        label = "int64" if signed else "uint64"
    if more(s, bs := read_until(s, label, until, 20)):
        return bs
    if signed:
        if re.match(b"[+-]?[0-9]+", bs):
            return int(bs)
    else:
        if re.match(b"[+]?[0-9]+", bs):
            return int(bs)
    return unexpected(s, label, bs)
    # bs = b""
    # for n in range(20):
    #     if not (b := peekb(s)):
    #         if len(bs) > 0:
    #             break
    #         return expected(s, label)
    #     if not b.isdigit():
    #         if n == 0 and signed and b in [b"+", b"-"]:
    #             pass
    #         else:
    #             break
    #     bs += readb(s)
    # else:
    #     return err(s, "int64 overrun")
    # return int(bs)

def read_uint64(s: Reader, until: bytes = b"\r\n"):
    return read_int64(s, signed=False, until=until)

def more(s: Reader, val):
    return val is not None and s.more == val

@defreader(b"$")
def read_blob_string(s: Reader, char=b"$", label="blob string"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, size := read_uint64(s)):
        return size
    if more(s, it := read_crlf(s)):
        return it
    if more(s, val := readb(s, size)):
        return val
    if more(s, it := read_crlf(s)):
        return it
    return val

@defreader(b"!")
def read_blob_error(s: Reader, char=b"!", label="blob error"):
    return read_blob_string(s, char=char, label=label)

@defreader(b"=")
def read_verbatim_string(s: Reader, char=b"=", label="verbatim string"):
    return read_blob_string(s, char=char, label=label)

@defreader(b"+")
def read_simple_string(s: Reader, char=b"+", label="simple string"):
    if more(s, it := expect(s, label, char)):
        return it
    bs = []
    while True:
        b = peekb(s)
        if not b:
            return expected(s, label)
        if b == b"\r":
            break
        if b == b"\n":
            return unexpected(s, label, b"\n")
        bs.append(readb(s))
    if more(s, it := read_crlf(s)):
        return it
    return b"".join(bs)

@defreader(b"-")
def read_simple_error(s: Reader):
    return read_simple_string(s, b"-", "simple error")

@defreader(b":")
def read_number(s: Reader, char=b":", label="number"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, n := read_int64(s, True)):
        return n
    if more(s, it := read_crlf(s)):
        return it
    return n

@defreader(b",")
def read_double(s: Reader, char=b",", label="double"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, bs := read_until(s, label, b"\r\n", 256)):
        return bs
    if more(s, it := read_crlf(s)):
        return it
    try:
        return float(bs)
    except ValueError:
        pass
    # TODO: exponential format should be invalid
    return unexpected(s, label, bs)

@defreader(b"_")
def read_null(s: Reader, char=b"_", label="null"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, it := read_crlf(s)):
        return it
    return None

@defreader(b"#")
def read_boolean(s: Reader, char=b"#", label="boolean"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, bs := read_until(s, label, b"\r\n", 2)):
        return bs
    if more(s, it := read_crlf(s)):
        return it
    if bs == b"t":
        return True
    if bs == b"f":
        return False
    return unexpected(s, "t or f", bs)

@defreader(b"(")
def read_bignum(s: Reader, char=b"(", label="bignum"):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, bs := read_until(s, label, b"\r\n", 65536)):
        return bs
    if more(s, it := read_crlf(s)):
        return it
    try:
        return int(bs)
    except ValueError:
        return unexpected(s, label, bs)

@defreader(b"*")
def read_array(s: Reader, char=b"*", label="array", cls: Type = tuple) -> Tuple:
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, size := read_int64(s, label="array size")):
        return it
    if more(s, it := read_crlf(s)):
        return it
    r = []
    for i in range(size):
        if more(s, it := read(s)):
            return it
        r.append(it)
    return cls(r)

@defreader(b"~")
def read_set(s: Reader, char=b"~", label="set", cls=set):
    return read_array(s, char=char, label=label, cls=cls)

@defreader(b"%")
def read_map(s: Reader, char=b"%", label="map", cls: Type[MutableMapping] = dict):
    if more(s, it := expect(s, label, char)):
        return it
    if more(s, size := read_int64(s, label="map size")):
        return it
    if more(s, it := read_crlf(s)):
        return it
    r = cls()
    for i in range(size):
        if more(s, key := read(s)):
            return key
        if more(s, val := read(s)):
            return val
        r[key] = val
    return r

class Attributes(NamedTuple):
    props: map
    value: Any

@defreader(b"|")
def read_attributes(s: Reader, char=b"|", label="attributes", cls: Type[Attributes] = Attributes):
    if more(s, props := read_map(s, char=char, label=label)):
        return props
    if more(s, value := read(s)):
        return value
    return cls(props, value)

@defreader(b">")
def read_push(s: Reader, char=b">", label="push"):
    if more(s, data := read_array(s, char=char, label=label)):
        return data
    if len(data) <= 0:
        return unexpected(s, label, data)
    kind = data[0]
    if not isinstance(kind, bytes):
        return unexpected(s, label, kind)
    if s.push:
        s.push(data)
    else:
        return err(s, "no push handler", data)
    return read(s)

def read(s: Reader):
    if not (it := peekb(s)):
        return expected(s, "RESP3 character code")
    if it not in readtable:
        return unexpected(s, "RESP3 character code", it)
    return readtable[it](s)

def read_from_string(string: Union[str, bytes], *args, **kws):
    if isinstance(string, str):
        string = string.encode("latin1")
    reader = Reader(io.BufferedReader(io.BytesIO(string)), *args, **kws)
    return read(reader)

def read_from_test_string(string: str, *args, **kws):
    string = string.replace("\r", "")
    string = string.replace("\n", "")
    string = string.replace(" ", "")
    string = string.replace("\t", "")
    string = string.replace("<CR>", "\r")
    string = string.replace("<LF>", "\n")
    return read_from_string(string, *args, **kws)
