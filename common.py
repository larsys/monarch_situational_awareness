import socket
import string
import time


class SAMTypeException(Exception):
    """An exception that is thrown when there are data type-related errors in
    the SAR"""
    def __init__(self, type_str, msg):
        self.type_str = type_str
        self.msg = msg

    def __str__(self):
        return "SAM Type Exception with %s: %s" % (self.type_str, self.msg)


class SAMConnectionException(Exception):
    """An exception that is thrown when there are connection errors in the
    SAR"""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "SAM Connection Exception: %s" % self.msg


def sanitize_name(name):
    """
    Transforms a string into a rosgraph-compatible representation.
    @param name: The string to sanitize
    """
    new_name = string.strip(string.lower(name))
    illegal_chars = string.whitespace + string.punctuation
    trans = string.maketrans(illegal_chars, '_'*len(illegal_chars))
    new_name = new_name.translate(trans)
    return new_name


def specify_name(name, hostname):
    return "[%s] %s" % (hostname, name)


def unspecify_name(name):
    return name[name.find("]")+2:]


def scramble_string(s):
    socket.gethostname()+s
    a = socket.gethostname()+s
    b = repr(int(time.time()*1e6))
    if len(a) < len(b):
        a = ' '*(len(b)-len(a)) + a

    c = ''.join("{:02x}".format(ord(x) ^ ord(y)) for x, y in zip(a, b))
    return c


class ClientID():
    def __init__(self,
                 name,
                 host):
        self.name = name
        self.host = host

    def __eq__(self, other):
        return (self.name == other.name and self.host == other.host)

    def __hash__(self):
        return hash((self.name, self.host))


class ClientProps():
    """
    A struct that is used for bookkeeping information regarding that SAR
    clients, namely the slots that the client reads from and writes to.
    """
    def __init__(self,
                 reads_from=None,
                 writes_to=None):
        """
        Constructor.
        @param reads_from: The set of slot IDs (SIDs) that this client reads
        @param writes_to: The SID of the slot that this client writes to
        """
        if reads_from is None:
            self.reads_from = set()
        else:
            self.reads_from = reads_from
        if writes_to is None:
            self.writes_to = set()
        else:
            self.writes_to = writes_to
