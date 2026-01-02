import json
import queue
import socket
import sys
import threading
from queue import Queue
import select
import time

import psutil
import tensorflow as tf

from node_state import NodeState, socket_recv, socket_send
from node_state import StateEnum

import zfpy
import lz4.frame

# 6000 is data, 6001 is model config(weights + architecture), 6003 is results
DATA_PORT = 6000 # receive input data
CONFIG_PORT = 6001 # receive model config(weights + architecture)
RESULT_PORT = 6003 # send results

class Node:
    def __init__(self) -> None:
        # task queue for incoming jobs
        self.job_queue = queue.Queue(maxsize=10)

    """ heartbeat registration with etcd """
    def _register_worker(self, node_state: NodeState):
        # 1. create a lease with a TTL
        lease = node_state._etcd.lease(ttl=10)  # 10 seconds TTL

        # 2. register the worker with the lease
        load_data = json.dumps({"status": node_state._state_enum.name , "load": self.get_system_metrics()})
        node_state._etcd.put(node_state._worker_key, load_data, lease=lease)
        print(f"Worker registered to etcd with key: {node_state._worker_key}")

        # 3. start a background thread to keep the lease alive periodically
        threading.Thread(target=self._keep_alive, args=(lease, node_state), daemon=True).start()

    """Get system metrics such as memory usage percentage. Mainly used for load balancing."""
    def get_system_metrics(self):
        # Simplified to just return memory percent for now
        v_mem = psutil.virtual_memory()
        mem_percent = v_mem.percent
        return round(mem_percent, 3)

    def _keep_alive(self, lease, node_state: NodeState):
        while True:
            try:
                load_data = json.dumps({"status": node_state._state_enum.name, "load": self.get_system_metrics()})
                node_state._etcd.put(node_state._worker_key,load_data,lease=lease) # update with load info
                lease.refresh()  # refresh the lease to keep it alive
                time.sleep(lease.ttl / 2)  # sleep for half the TTL before refreshing again
            except Exception as e:
                print(f"Keep-Alive failed: {e}")
                break

    """
        Runs a server that listens for and receives a complete model partition configuration
        (architecture JSON, worker index, and weights) from the Dispatcher.
        It constructs the model and places it into the job_queue for the data worker.

        :param self: The containing object instance.
        :param node_state: Object containing configuration like chunk_size and the node's current state.
    """
    def _config_socket(self, node_state: NodeState):
        while True:
            config_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            config_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                # bind and listen 6001
                config_server.bind(("0.0.0.0", CONFIG_PORT))
                print(f"Config socket running on {CONFIG_PORT}, waiting for dispatch...")
                config_server.listen(1)

                # block until a connection is made
                client_sock, _ = config_server.accept()
                client_sock.setblocking(1)

                # 1. receive model architecture json
                model_json = socket_recv(client_sock, node_state.chunk_size)

                # 2. receive worker index (1 byte)
                worker_index_bytes = socket_recv(client_sock, chunk_size=1)

                # Validate received data
                if not model_json or not worker_index_bytes:
                    print("Incomplete config received. Restarting listen.")
                    client_sock.close()
                    continue

                worker_index = int(worker_index_bytes.decode())

                # 3. receive weights
                weights_data = self._recv_weights(client_sock, node_state.chunk_size)

                # 4. construct model partition
                part = tf.keras.models.model_from_json(model_json)
                part.set_weights(weights_data)

                print(f"Model Partition {worker_index} loaded. Enqueuing job...")
                self.job_queue.put((worker_index, part))
                node_state._state_enum = StateEnum.BUSY

                # Optional: save visualization
                # tf.keras.utils.plot_model(part, f"model_{id}_{worker_index}.png")

                # 5. send ACK
                client_sock.send(b'\x06')  # ACK
                time.sleep(0.05) # ensure ACK is sent before closing
                client_sock.close()
                print("Model & Weights received and loaded successfully. Ready for inference.")

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Config socket error: {e}. Restarting listen.")
            finally:
                config_server.close()

    """ Receive weights over socket """
    def _recv_weights(self, sock: socket.socket, chunk_size: int):
        size_left = 8
        byts = bytearray()
        while size_left > 0:
            try:
                recv = sock.recv(min(size_left, 8))
                size_left -= len(recv)
                byts.extend(recv)
            except socket.error as e:
                if e.errno != socket.EAGAIN:
                    raise e
                select.select([sock], [], [])
        array_len = int.from_bytes(byts, 'big')

        weights = []
        for i in range(array_len):
            recv = bytes(socket_recv(sock, chunk_size))
            weights.append(self._decomp(recv))
        return weights

    """ Helper Functions for Compression/Decompression """
    def _comp(self, arr):
        return lz4.frame.compress(zfpy.compress_numpy(arr))
    def _decomp(self, byts):
        return zfpy.decompress_numpy(lz4.frame.decompress(byts))

    """
    Runs the data receiving server. This server accepts incoming connections
    (typically from a Dispatcher), reads a 4-byte index header, receives the
    data payload using socket_recv, decompresses it, and puts the (index, data)
    pair into the processing queue (to_send).

    :param self: The containing object instance (e.g., a distributed node).
    :param node_state: Object containing configuration like chunk_size.
    :param to_send: The queue to place the received (target_index, input_data) for processing.
    """
    def _data_server(self, node_state: 'NodeState', to_send: Queue):
        chunk_size = node_state.chunk_size
        # 1. Socket Initialization and Setup
        data_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Allows the socket to be immediately re-bound after closure.
        data_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            # Bind the socket to all available interfaces on the predefined port.
            data_server.bind(("0.0.0.0", DATA_PORT))
            data_server.listen(5)  # Set connection queue limit
            print("Data server running...")
        except Exception as e:
            print(f"Bind error: {e}")
            return

        # 2. Main Server Loop
        while True:
            data_cli = None
            try:
                # Block and wait for an incoming client connection.
                data_cli, addr = data_server.accept()
                # Set a timeout for the connected client socket to prevent infinite blocking on I/O.
                data_cli.settimeout(15)

                # --- Data Reception Logic ---
                # 1. Read the 4-byte custom header (Target Index)
                # This is a raw read, *not* using socket_recv, as it doesn't have the 8-byte length header.
                header_data = b''
                while len(header_data) < 4:
                    # Read the remaining bytes needed for the 4-byte header.
                    recv = data_cli.recv(4 - len(header_data))
                    if not recv:
                        # Connection closed by peer before reading the full header.
                        raise socket.error("Connection closed while reading header")
                    header_data += recv

                # Convert the 4 bytes (big-endian) into the target partition index.
                target_index = int.from_bytes(header_data, 'big')
                # print(f"DEBUG: Received Data Header for Partition {target_index}")

                # 2. Read the main data payload
                # The Dispatcher used 'socket_send' (with an 8-byte length header) to send this data.
                # Therefore, we must use the robust 'socket_recv' function here.
                data = bytes(socket_recv(data_cli, chunk_size))

                if data:
                    # Decompress/deserialize the received raw bytes.
                    inpt = self._decomp(data)
                    # Enqueue the received data (index and input) for the worker thread.
                    to_send.put((target_index, inpt))

            except socket.timeout:
                # A timeout occurred on the client connection; this is not a fatal server error.
                print("Data receive timeout")
            except socket.error as e:
                # Handle connection-specific errors (e.g., reset by peer).
                print(f"Data server connection error: {e}")
            except Exception as e:
                # Handle any other unexpected errors during processing.
                print(f"Unexpected error in data server: {e}")
            finally:
                # Ensure the client connection is closed whether successful or not.
                if data_cli:
                    data_cli.close()

    """
    The main worker thread loop for processing data packets.
    It fetches data, ensures the correct model (worker/partition) is loaded,
    performs inference, and sends the result back.

    :param self: The containing object instance (e.g., a distributed node).
    :param node_state: An object tracking the current state (IDLE/BUSY) of this node.
    :param to_send: The input queue from the Dispatcher containing incoming data packets
                    (target_index, input_data).
    """
    def _data_client(self, node_state: 'NodeState', to_send: Queue):
        print("Data Client started. Waiting for jobs...")

        current_model = None
        current_worker_index = None

        while True:
            # 1. Attempt to fetch a data packet (target_index, input_data)
            # We prioritize fetching data first, then check if the model matches.

            try:
                # Block and wait for a new data packet from the input queue.
                target_index, inpt = to_send.get()
                # Mark the data item as processed from the input queue
                to_send.task_done()
            except Exception:
                # Handle potential exceptions during queue retrieval (e.g., queue closed)
                continue

            # 2. Model Matching Check Loop
            while True:
                # Scenario A: No model is currently loaded, or the loaded model
                # does not match the data's required partition/target index.
                if current_worker_index != target_index:
                    print(
                        f"Data received for Partition {target_index}, but current is {current_worker_index}. Fetching correct model...")

                    # If the model does not match, block and wait on the job_queue
                    # until the correct model (Config) is received from the Dispatcher.

                    # NOTE: It's assumed that the Dispatcher ensures correct ordering
                    # (Config/Model is sent before its corresponding Data).

                    # Block and wait for the new model/worker configuration.
                    # The job_queue typically holds (index, model_object) tuples.
                    new_index, new_model = self.job_queue.get()

                    # Update the current state with the new model configuration.
                    current_worker_index = new_index
                    current_model = new_model
                    node_state._state_enum = StateEnum.BUSY  # State reflects that a new job is being configured/started
                    self.job_queue.task_done()  # Mark the job item as processed

                    print(f"Switched to Partition {current_worker_index}.")

                    # After receiving the new model, the loop naturally re-checks
                    # (current_worker_index == target_index), which should now be true.

                else:
                    # Scenario B: The loaded model matches the data's required partition index.
                    break  # Exit the internal loop to proceed to inference.

            # 3. Execute Inference
            try:
                node_state._state_enum = StateEnum.BUSY
                # print(f"Processing P{target_index} data...")

                # Perform the forward pass using the loaded model.
                output_tensor = current_model(inpt, training=False)
                output = output_tensor.numpy()

                # Apply post-processing/compression logic defined in _comp.
                out = self._comp(output)

                # Send the final result back to the Dispatcher.
                self._send_result_to_dispatcher(node_state, target_index, out)

                print(f"Partition {target_index} finished. Worker IDLE.")
                node_state._state_enum = StateEnum.IDLE

            except Exception as e:
                print(f"Inference Error: {e}")
                import traceback
                traceback.print_exc()

    def _send_result_to_dispatcher(self, node_state, worker_index, out_data):
        # Send result back to dispatcher on port 6003
        data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # bind to the worker's registered IP address (to avoid using wrong interface)
            data_sock.bind((node_state._my_ip, 0))

            data_sock.connect((node_state.dispatcher_ip, RESULT_PORT))
            index_bytes = worker_index.to_bytes(1, 'big')
            socket_send(index_bytes, data_sock, chunk_size=1)
            socket_send(out_data, data_sock, node_state.chunk_size)
        except Exception as e:
            print(f"Failed to send result: {e}")
        finally:
            data_sock.close()

    def run(self, dispatcher_ip: str):
        ns = NodeState(chunk_size=512 * 1000, dispatcher_ip=dispatcher_ip)
        self._register_worker(ns)

        # Start config socket thread 6001
        config_thread = threading.Thread(target=self._config_socket, args=(ns,), daemon=True)

        to_send = queue.Queue(1000)

        # Start data server and data client threads 6000
        dserv = threading.Thread(target=self._data_server, args=(ns, to_send), daemon=True)
        dcli = threading.Thread(target=self._data_client, args=(ns, to_send), daemon=True)

        config_thread.start()
        dserv.start()
        dcli.start()

        # Keep the main thread alive to let daemon threads run
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Worker shutting down...")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python node.py <dispatcher_ip>")
        sys.exit(1)
    node = Node()
    node.run(sys.argv[1])

