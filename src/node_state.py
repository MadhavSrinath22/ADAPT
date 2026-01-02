import select
import socket
from enum import Enum

import etcd3
from pynvml import *


class NodeState:
    def __init__(self, chunk_size, dispatcher_ip) -> None:
        # basic state info
        self._chunk_size = chunk_size
        self._next_node = ""
        self._state_enum = StateEnum.IDLE

        # etcd client info
        self.dispatcher_ip = dispatcher_ip
        self._etcd = etcd3.client(host=dispatcher_ip, port=2379)
        self._my_ip = self._get_local_ip()
        self._worker_key = f'/workers/{self._my_ip}'

        # threading lock
        self._lock = threading.Lock()

    def _get_local_ip(self):
        """Get the local non-loopback IP address by connecting to an external address."""
        try:
            # Create a socket and connect to an external address (doesn't actually send data)
            # Using the dispatcher IP ensures we get the IP on the correct network interface
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((self.dispatcher_ip, 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception as e:
            print(f"Failed to auto-detect IP: {e}, falling back to hostname lookup")
            # Fallback to hostname lookup
            return socket.gethostbyname(socket.gethostname())

    @property
    def chunk_size(self):
        with self._lock:
            return self._chunk_size
    @property
    def next_node(self):
        with self._lock:
            return self._next_node
    @next_node.setter
    def next_node(self, nx):
        with self._lock:
            self._next_node = nx


def socket_send(bytes_to_send: bytes, sock: socket.socket, chunk_size: int):
    """
    Sends all bytes over a non-blocking socket in chunks, handling EWOULDBLOCK/EAGAIN errors
    by using select.select() to wait until the socket is ready for writing.
    The data size is sent first as an 8-byte big-endian header.

    :param bytes_to_send: The data to be sent (as bytes).
    :param sock: The non-blocking socket object to send data through.
    :param chunk_size: The maximum size of data to attempt to send in a single call.
    :raises socket.error: If any socket error other than EAGAIN/EWOULDBLOCK occurs.
    """
    # Get the total size of the data and convert it to an 8-byte big-endian integer.
    size = len(bytes_to_send)
    size_bytes = size.to_bytes(8, 'big')

    # --- Phase 1: Send the 8-byte size header ---
    while len(size_bytes) > 0:
        try:
            # Attempt to send the remaining part of the size header.
            sent = sock.send(size_bytes)
            # Slice the size_bytes array to remove the bytes that were successfully sent.
            size_bytes = size_bytes[sent:]
        except socket.error as e:
            # Check for EWOULDBLOCK or EAGAIN, indicating the buffer is full.
            if e.errno != socket.EAGAIN:
                raise e
            # The socket is temporarily blocked; wait until it is writable.
            # select.select waits for an event on the socket (here, readiness for writing).
            select.select([], [sock], [])

    # --- Phase 2: Send the main data body in chunks ---
    for i in range(0, len(bytes_to_send), chunk_size):
        # Determine the current chunk to send.
        if len(bytes_to_send) - i < chunk_size:
            chunk = bytes_to_send[i:]
        else:
            chunk = bytes_to_send[i:i + chunk_size]

        # Continuously send the chunk until it's completely transmitted.
        while len(chunk) > 0:
            try:
                # Attempt to send a part of the current chunk.
                cs = sock.send(chunk)
                # Slice the chunk to remove the bytes that were successfully sent.
                chunk = chunk[cs:]
            except socket.error as e:
                # Handle non-blocking error (buffer full).
                if e.errno != socket.EAGAIN:
                    raise e
                # Wait until the socket is ready for writing again.
                select.select([], [sock], [])


def socket_recv(sock: socket.socket, chunk_size: int) -> bytes:
    """
    Receives all data from a non-blocking socket, first reading an 8-byte size header,
    then reading the data body in chunks. It handles EWOULDBLOCK/EAGAIN errors using
    select.select() to wait until the socket is ready for reading.

    :param sock: The non-blocking socket object to receive data from.
    :param chunk_size: The maximum size of data to attempt to receive in a single call.
    :return: The complete received data as bytes. Returns b'' if the connection is
             gracefully closed before any data is received.
    :raises socket.error: If the connection is closed abruptly (incomplete header or data).
    """
    size_left = 8
    byts = bytearray()

    # --- Phase 1: Read the 8-byte data length header ---
    while size_left > 0:
        try:
            # Attempt to receive data, up to the remaining size or 8 bytes.
            recv = sock.recv(min(size_left, 8))

            # Check for EOF (End of File), indicated by an empty bytes object.
            if not recv:
                # If no bytes were received at all (byts is empty),
                # it means a clean connection closure.
                if len(byts) == 0:
                    return b''
                else:
                    # If the connection closed mid-header, raise an error.
                    raise socket.error("Incomplete header received, connection closed.")

            size_left -= len(recv)
            byts.extend(recv)
        except socket.error as e:
            # Handle non-blocking error (no data immediately available).
            if e.errno != socket.EAGAIN:
                raise e
            # Wait until the socket is ready for reading.
            select.select([sock], [], [])

    # Convert the 8-byte header into the expected total data size integer.
    data_size = int.from_bytes(byts, 'big')

    # --- Phase 2: Read the main data body ---
    data_json = bytearray(data_size)
    data_counter = 0
    left = data_size

    while left > 0:
        try:
            # Attempt to receive the next chunk of data.
            recv = sock.recv(min(left, chunk_size))

            # Check for EOF in the middle of data reception.
            if not recv:
                raise socket.error("Incomplete data received, connection closed.")

            left -= len(recv)
            # Place the received bytes into the correct position in the final data array.
            data_json[data_counter:data_counter + len(recv)] = recv
            data_counter += len(recv)
        except socket.error as e:
            # Handle non-blocking error.
            if e.errno != socket.EAGAIN:
                raise e
            # Wait until the socket is ready for reading again.
            select.select([sock], [], [])

    # The result is returned as a immutable bytes object.
    return bytes(data_json)

# Enum for node states
class StateEnum(Enum):
    IDLE = 0
    BUSY = 1
    UNKNOWN = 2
    PARSE_ERROR = 3