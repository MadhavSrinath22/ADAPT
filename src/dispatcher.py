import json

import etcd3
import select
import socket
import threading
from typing import List
import queue

from src.dag_util import *
from src.node_state import socket_recv, socket_send, StateEnum

import lz4.frame
import zfpy
import time

# 6000 is data, 6001 is model config(weights + architecture), 6003 is results
DATA_PORT = 6000 # send input data
CONFIG_PORT = 6001 # send model config(weights + architecture)
RESULT_PORT = 6003 # receive results


class DEFER:
    def __init__(self) -> None:
        # basic config
        self.dispatchIP = socket.gethostbyname(socket.gethostname())
        self.chunk_size = 512 * 1000
        self.worker_prefix = '/workers/' # ectd key prefix for workers
        self.etcd = etcd3.client(host=self.dispatchIP)

        # heartbeat monitoring
        self.worker_lock = threading.RLock()  # lock for computeNodes
        self.computeNodes: List[str] = []  # current available worker IPs
        self.models_to_dispatch = None  # models to be dispatched
        self._shutdown_event = threading.Event()  # for clean shutdown

        # partition map and worker load/status tracking
        self.partition_map: dict = {}
        self.worker_loads: dict = {}
        self.worker_statuses: dict = {}
        self.dispatch_cooldowns = {} # worker cooldowns to prevent rapid re-assignments
        self.ETCD_SYNC_DELAY = 2 # seconds, time to wait for etcd to sync worker status
        self.reserved_workers = set() # local reservation of workers being assigned, for resloving nanosecond-level race

        # use semaphore to limit concurrent inferences
        self.MAX_CONCURRENCY = 2 # max concurrent inferences (the degree of pineline parallelism)
        self.concurrency_sem = threading.Semaphore(self.MAX_CONCURRENCY)

        # in-flight task registry for watchdog
        self.inflight_tasks = {} # Key: worker_ip, Value: {'partition': int, 'data': bytes, 'start_time': float}
        self.inflight_lock = threading.Lock()
        self.TASK_TIMEOUT = 20.0 # retry tasks after 20 seconds

        # penalty strategy(Key: worker_ip, Value: float (>= 0))
        # 0 refer to no penalty (perfect reputation), higher is worse(poor reputation)
        self.worker_penalties = {}
        # penalty decay factor, when successful, multiply current penalty by this factor
        self.PENALTY_DECAY = 0.5
        #  fixed penalty to add on failure, equivalent to adding this much load
        self.FAILURE_PENALTY = 50.0
        # circuit breaker threshold, if penalty exceeds this, worker is skipped
        self.CIRCUIT_BREAKER_THRESHOLD = 200.0
        # last decay time for penalties
        self.last_decay_time = {}

    """ Update worker reputation based on success or failure """
    def _update_reputation(self, worker_ip, is_success):
        with self.worker_lock:
            current_penalty = self.worker_penalties.get(worker_ip, 0.0)

            if is_success:
                # when success, exponentially decay penalty
                self.worker_penalties[worker_ip] = max(0.0, current_penalty * self.PENALTY_DECAY)
                # if penalty is very low, reset to 0
                if self.worker_penalties[worker_ip] < 1.0:
                    self.worker_penalties[worker_ip] = 0.0
            else:
                # if failure, add fixed penalty
                self.worker_penalties[worker_ip] = current_penalty + self.FAILURE_PENALTY

            print(f"Reputation Update: {worker_ip} -> Penalty: {self.worker_penalties[worker_ip]:.2f}")




    """
    A separate thread that monitors all in-flight tasks for timeouts.
    If a task exceeds the predefined TASK_TIMEOUT, it is marked for removal 
    from the in-flight list and automatically rescheduled.
    """
    def _task_watchdog(self):
        print("Starting Task Watchdog...")
        while not self._shutdown_event.is_set():
            time.sleep(1)

            tasks_to_retry = []

            with self.inflight_lock:
                current_time = time.time()
                ## check for timed-out tasks
                for task_key, info in list(self.inflight_tasks.items()):
                    worker_ip, partition = task_key  # unpack key

                    if current_time - info['start_time'] > self.TASK_TIMEOUT:
                        print(f"WATCHDOG: Task on {worker_ip} (P{info['partition']}) TIMED OUT!")
                        self._update_reputation(worker_ip, is_success=False) # update reputation on timeout
                        tasks_to_retry.append(info)
                        del self.inflight_tasks[task_key]

            # reschedule timed-out tasks outside the lock
            for task in tasks_to_retry:
                print(f"WATCHDOG: Rescheduling Partition {task['partition']}...")

                # start a new thread to retry the task
                threading.Thread(target=self._retry_task, args=(task,), daemon=True).start()

    """ Retry a timed-out task """
    def _retry_task(self, task_info):
        partition = task_info['partition']
        data = task_info['data']

        # retry from the failed partition
        # note that _wait_and_forward will acquire a new worker
        # so we just call it directly
        self._wait_and_forward(partition, data)

    """ Get list of available workers from etcd """
    def _get_available_workers(self) -> List[str]:
        # get list of available workers from etcd
        workers = []
        for value, metadata in self.etcd.get_prefix(self.worker_prefix):
            workers.append(metadata.key.decode('utf-8').replace(self.worker_prefix, ''))
        # return sorted list for consistency
        return sorted(workers)

    """ Get worker loads and statuses from etcd """
    def _get_worker_loads_and_statuses(self) -> tuple[dict, dict]:
        # get worker loads and statuses from etcd
        worker_loads = {}
        worker_statuses = {}
        for value, metadata in self.etcd.get_prefix(self.worker_prefix):
            ip = metadata.key.decode('utf-8').replace(self.worker_prefix, '')
            try:
                # parse JSON info from worker heartbeat
                data = json.loads(value.decode('utf-8'))
                load = data.get("load", 0)  # default to 0 load
                status = data.get("status", StateEnum.UNKNOWN.name)  # default to UNKNOWN status

                worker_loads[ip] = load
                worker_statuses[ip] = status

            except Exception:
                # parsing error, set defaults
                worker_loads[ip] = 0
                worker_statuses[ip] = StateEnum.PARSE_ERROR.name  # indicate parse error

        return worker_loads, worker_statuses

    """ Worker monitor thread function"""
    def _worker_monitor(self):
        print("Starting Worker Monitor...")
        while not self._shutdown_event.is_set():
            try:
                # 1. set up watch on worker prefix
                self.etcd.watch_prefix(self.worker_prefix)

                # 2. update loads and statuses
                new_worker_loads, new_worker_statuses = self._get_worker_loads_and_statuses()
                new_workers = sorted(new_worker_loads.keys())
                partitions_to_recover = {}
                with self.worker_lock:
                    # update current worker info
                    old_workers = self.computeNodes
                    self.computeNodes = new_workers
                    self.worker_loads = new_worker_loads
                    self.worker_statuses = new_worker_statuses

                    # 3. check for changes in worker set
                    if set(old_workers) != set(new_workers):
                        failed_workers = set(old_workers) - set(new_workers)
                        if failed_workers and self.models_to_dispatch:
                            print(f"Worker(s) failed: {failed_workers}. Reconfiguring pipeline.")
                            for index, ip in self.partition_map.items():
                                if ip in failed_workers:
                                    # record partitions to recover
                                    partitions_to_recover[index] = self.models_to_dispatch[index - 1]
                                    # remove from live partition map
                                    del self.partition_map[index]

                if partitions_to_recover:
                    # start recovery outside lock
                    self._re_dispatch_on_failure(partitions_to_recover)
            except Exception as e:
                if not self._shutdown_event.is_set():
                    print(f"Etcd watch error: {e}. Retrying in 5s.")
                    time.sleep(5)
            time.sleep(0.5) # avoid tight loop and obtain lock frequently

    """ Select the lowest load IDLE worker for a partition """
    def _select_lowest_load_worker_for_partition(self) -> str or None:
        with self.worker_lock:
            if not self.computeNodes:
                return None

            current_time = time.time()
            candidates = []

            for ip in self.computeNodes:
                status = self.worker_statuses.get(ip)
                cooldown_until = self.dispatch_cooldowns.get(ip, 0)

                # check circuit breaker
                current_penalty = self.worker_penalties.get(ip, 0.0)
                if current_penalty > self.CIRCUIT_BREAKER_THRESHOLD:
                    # check if we can decay penalty
                    last_decay = self.last_decay_time.get(ip, 0)
                    if current_time - last_decay > 1.0:
                        self.worker_penalties[ip] = current_penalty * 0.9
                        self.last_decay_time[ip] = current_time
                        print(
                            f"Worker {ip} recovering from circuit break... (Penalty: {self.worker_penalties[ip]:.2f})")
                    continue

                if status == StateEnum.IDLE.name and ip not in self.reserved_workers:
                    if current_time > cooldown_until:
                        # get raw load
                        raw_load = self.worker_loads.get(ip, 0)

                        # calculate effective load (physical load + penalty)
                        effective_load = raw_load + current_penalty

                        candidates.append((effective_load, ip))

            if not candidates:
                return None

            # pick the one with the lowest effective load
            candidates.sort(key=lambda x: x[0])
            lowest_load_worker_ip = candidates[0][1]

            self.reserved_workers.add(lowest_load_worker_ip)
            self.dispatch_cooldowns[lowest_load_worker_ip] = time.time() + self.ETCD_SYNC_DELAY

            return lowest_load_worker_ip

    """ Acquire and configure a worker for a partition """
    def _acquire_and_configure_worker(self, partition_index: int) -> str:
        # get model segment for this partition
        model_segment = self.models_to_dispatch[partition_index - 1]
        print(f"Scheduling Partition {partition_index}...")

        # while loop until successful configuration or shutdown
        while not self._shutdown_event.is_set():
            # 1. pick a worker
            worker_ip = self._select_lowest_load_worker_for_partition()

            # no available worker, wait and retry
            if not worker_ip:
                time.sleep(0.1)
                continue

            # 2. try to configure the worker
            try:
                print(f"Worker {worker_ip} selected for Partition {partition_index}. Deploying model...")

                # send full configuration
                self._send_full_configuration(worker_ip, model_segment, partition_index)

                # successfully configured, update partition map
                with self.worker_lock:
                    self.partition_map[partition_index] = worker_ip
                    # remove from reserved set
                    if worker_ip in self.reserved_workers:
                        self.reserved_workers.remove(worker_ip)

                return worker_ip  # return the configured worker IP

            except Exception as e:
                # if configuration fails, log and retry
                print(f"Error configuring worker {worker_ip} for P{partition_index}: {e}. Retrying...")
                self._update_reputation(worker_ip, is_success=False)

                # release reservation (avoid deadlock)
                with self.worker_lock:
                    if worker_ip in self.reserved_workers:
                        self.reserved_workers.remove(worker_ip)

                # 2. short delay before retrying (avoid tight loop)
                time.sleep(0.5)

                # 3. continue retrying
                continue
        # shutdown event set, exit
        return None

    """re dispatch models on worker failure"""
    def _re_dispatch_on_failure(self, partitions_to_recover: dict):
        print(f"Attempting to recover partitions: {list(partitions_to_recover.keys())}")

        new_assignments = {}  # {partition_index: new_worker_ip}

        # 1. choose new workers for each partition to recover
        for index, model_segment in partitions_to_recover.items():

            # get new worker for this partition
            new_worker_ip = self._select_lowest_load_worker_for_partition()

            if not new_worker_ip:
                print(
                    f"CRITICAL: Failed to find an available worker for recovery of partition {index}. Inference stalled.")

                # roll back any reservations made so far
                if new_assignments:
                    print("Rolling back reservations due to partial failure...")
                    with self.worker_lock:
                        for reserved_ip in new_assignments.values():
                            if reserved_ip in self.reserved_workers:
                                self.reserved_workers.remove(reserved_ip)
                return

            # 2. record new assignment
            with self.worker_lock:
                new_assignments[index] = new_worker_ip
                self.partition_map[index] = new_worker_ip
                print(f"Partition {index} re-assigned to Worker: {new_worker_ip}")

        # 3. start re-dispatch in a separate thread
        re_dispatch_thread = threading.Thread(target=self._perform_targeted_dispatch,
                                              args=(new_assignments, partitions_to_recover),
                                              daemon=True)
        re_dispatch_thread.start()

    """perform targeted dispatch to new workers"""
    def _perform_targeted_dispatch(self, assignments: dict, partitions: dict) -> None:
        for index, worker_ip in assignments.items():
            model_segment = partitions[index]
            try:
                # send full configuration
                self._send_full_configuration(worker_ip, model_segment, index)
                print(f"Partition {index} successfully RE-DISPATCHED to new Worker: {worker_ip}")
            except Exception as e:
                print(f"CRITICAL: Failed to re-dispatch Partition {index} to {worker_ip}: {e}")
            finally:
                # release reservation
                with self.worker_lock:
                    if worker_ip in self.reserved_workers:
                        self.reserved_workers.remove(worker_ip)

    """ Partition the model into segments based on specified layer names """
    def _partition(self, model: tf.keras.Model, layer_parts: List[str]) -> List[tf.keras.Model]:
        models = []
        for p in range(len(layer_parts) + 1):
            if p == 0:
                start = model.input._keras_history[0].name
            else:
                start = layer_parts[p-1]
            if p == len(layer_parts):
                print(model.output)
                end = model.output._keras_history[0].name
            else:
                end = layer_parts[p]
            part = construct_model(model, start, end, part_name=f"part{p+1}")
            models.append(part)
        return models

    """ Send model weights to a worker """
    def _send_weights(self, weights: List, sock: socket.socket, chunk_size: int):
        size = len(weights)
        size_bytes = size.to_bytes(8, 'big')
        while len(size_bytes) > 0:
            try:
                sent = sock.send(size_bytes)
                size_bytes = size_bytes[sent:]
            except socket.error as e:
                    if e.errno != socket.EAGAIN:
                        raise e
                    select.select([], [sock], [])
        for w_arr in weights:
                as_bytes = self._comp(w_arr)
                socket_send(as_bytes, sock, chunk_size)

    """ Helper functions for compression and decompression"""
    def _comp(self, arr):
        return lz4.frame.compress(zfpy.compress_numpy(arr))
    def _decomp(self, byts):
        return zfpy.decompress_numpy(lz4.frame.decompress(byts))

    """
    The main thread loop responsible for initiating the distributed inference pipeline.
    It manages concurrency using a Semaphore, fetches input data, acquires the 
    first worker (P1), registers the task, and sends the initial data packet.

    :param self: The Dispatcher instance.
    :param input_queue: The queue containing raw input data (e.g., images) to be processed.
    """
    def _startDistEdgeInference(self, input_queue: queue.Queue):
        print(f"Starting Pipeline Inference (Max Concurrent: {self.MAX_CONCURRENCY})...")

        while not self._shutdown_event.is_set():
            # 1. Acquire semaphore for concurrency control
            self.concurrency_sem.acquire()

            try:
                # 2. get input data
                try:
                    model_input = input_queue.get(timeout=1)
                except queue.Empty:
                    # if no input, release semaphore and continue
                    self.concurrency_sem.release()
                    continue

                # 3. acquire and configure P1 worker
                worker_ip = self._acquire_and_configure_worker(partition_index=1)

                if not worker_ip:
                    print("Critical: Failed to acquire worker for P1.")
                    # release semaphore and continue
                    self.concurrency_sem.release()
                    continue

                # register in-flight task
                with self.inflight_lock:
                    task_key = (worker_ip, 1)
                    self.inflight_tasks[task_key] = {
                        'partition': 1, # partition index
                        'data': self._comp(model_input), # compressed data
                        'start_time': time.time() # timestamp
                    }

                # send data to P1
                try:
                    data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    data_sock.connect((worker_ip, DATA_PORT))

                    # send target partition index as 4-byte big-endian integer
                    target_index = 1
                    data_sock.sendall(target_index.to_bytes(4, 'big'))
                    out = self._comp(model_input)
                    socket_send(out, data_sock, self.chunk_size)

                    data_sock.shutdown(socket.SHUT_WR)
                    data_sock.close()

                    print(f"Pipeline Started for a new image.")

                except Exception as e:
                    print(f"Error sending data to Worker 1: {e}")
                    # on failure, remove from in-flight and release semaphore
                    self.concurrency_sem.release()
                    self._update_reputation(worker_ip, is_success=False)
                    #  also remove from in-flight
                    if task_key in self.inflight_tasks:
                        del self.inflight_tasks[task_key]

            except Exception as e:
                print(f"Unexpected error in inference loop: {e}")
                # on any unexpected error, release semaphore
                self.concurrency_sem.release()


    """
    Runs an asynchronous server that listens for and receives intermediate or final 
    inference results from worker nodes. It uses select.select() to manage 
    multiple simultaneous connections efficiently.

    :param self: The Dispatcher instance.
    :param output_stream: The queue for placing the final prediction result.
    """
    def _intermediate_result_server(self, output_stream: queue.Queue):
        # set up server socket
        intermediate_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        intermediate_server.bind(("0.0.0.0", RESULT_PORT))
        intermediate_server.listen(5)
        worker_cli = {}

        print("Intermediate Result Server Started.")

        while not self._shutdown_event.is_set():
            # use select to wait for incoming connections or data
            readable, _, _ = select.select([intermediate_server] + list(worker_cli.keys()), [], [], 0.1)

            # handle readable sockets
            for s in readable:
                if s is intermediate_server:
                    conn, addr = intermediate_server.accept()
                    conn.setblocking(1)
                    worker_cli[conn] = addr[0]

                    raw_ip = addr[0]

                    # special handling for localhost mapping
                    self._update_localhost(conn, raw_ip, worker_cli)
                else:
                    # existing connection has data
                    current_worker_ip = worker_cli.get(s)

                    try:
                        # 1. receive worker index
                        worker_index_bytes = socket_recv(s, chunk_size=1)
                        if not worker_index_bytes:
                            raise socket.error("Empty read")

                        worker_index = int.from_bytes(worker_index_bytes, 'big')

                        # 2. receive data
                        data = bytes(socket_recv(s, self.chunk_size))

                        # close socket
                        if current_worker_ip:
                            completed_task_key = (current_worker_ip, worker_index)
                            with self.inflight_lock:
                                if completed_task_key in self.inflight_tasks:
                                    # print(f"DEBUG: Task {completed_task_key} finished. Removing from watchdog.")
                                    self._update_reputation(current_worker_ip, is_success=True)
                                    del self.inflight_tasks[completed_task_key]
                                else:
                                    print(f"WARNING: Could not find task {completed_task_key} to delete! Keys in registry: {list(self.inflight_tasks.keys())}")
                                    pass

                        # 3. Check logic
                        next_worker_index = worker_index + 1

                        num_partitions = len(self.models_to_dispatch) if self.models_to_dispatch else 0

                        if next_worker_index <= num_partitions:
                            # forward to next partition
                            t = threading.Thread(
                                target=self._wait_and_forward,
                                args=(next_worker_index, data),
                                daemon=True
                            )
                            t.start()
                        else:
                            # final partition, put result into output queue
                            print(f"Received final result from Partition {worker_index}. Putting into output queue.")
                            pred = self._decomp(data)
                            output_stream.put(pred)
                            print("Image processing complete. Releasing concurrency slot.")
                            self.concurrency_sem.release()

                    except socket.error as e:
                        s.close()
                        if s in worker_cli:
                            del worker_cli[s]
                    except Exception as e:
                        print(f"Unexpected error in server loop: {e}")
                        s.close()
                        if s in worker_cli:
                            del worker_cli[s]

    """ Update localhost mapping for incoming connections """
    def _update_localhost(self, conn, raw_ip, worker_cli):
        final_ip = raw_ip
        with self.worker_lock:
            # check if raw_ip is in computeNodes
            if raw_ip not in self.computeNodes and '127.0.0.1' in self.computeNodes:
                # map to localhost
                # print(f"DEBUG: Mapping incoming connection from {raw_ip} to 127.0.0.1")
                final_ip = '127.0.0.1'
        # update mapping
        worker_cli[conn] = final_ip

    """ Wait for an available worker and forward data to it """
    def _wait_and_forward(self, next_worker_index, data):
        # 1. dynamically acquire and configure the next worker
        target_ip = self._acquire_and_configure_worker(partition_index=next_worker_index)

        if not target_ip:
            print(f"Stopped forwarding P{next_worker_index} due to shutdown/error.")
            # release semaphore since we cannot proceed
            self.concurrency_sem.release()
            return

        # 2. register in-flight task
        task_key = (target_ip, next_worker_index)

        with self.inflight_lock:
            self.inflight_tasks[task_key] = {
                'partition': next_worker_index, # partition index
                'data': data, # raw data
                'start_time': time.time() # timestamp
            }

        # 3. send data asynchronously
        try:
            print(f"Async Forwarding data to P{next_worker_index} ({target_ip})...")
            self._forward_data_to_worker(target_ip, data, next_worker_index)
        except Exception as e:
            print(f"Error in async forwarding to {target_ip}: {e}")

    """ Forward data to next worker """
    def _forward_data_to_worker(self, worker_ip, data, target_index):
        forward_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            forward_sock.connect((worker_ip, DATA_PORT))

            # 1. send target partition index as 4-byte big-endian integer
            forward_sock.sendall(int(target_index).to_bytes(4, 'big'))

            # 2. send data
            socket_send(data, forward_sock, self.chunk_size)

            # 3. shutdown write
            forward_sock.shutdown(socket.SHUT_WR)
        except Exception as e:
            print(f"Error forwarding data to {worker_ip}: {e}")
        finally:
            forward_sock.close()

    """ Send full model configuration to a worker """
    def _send_full_configuration(self, worker_ip, model_part, worker_index_val):
        model_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 1. timeout for connect
        model_sock.settimeout(5)

        try:
            model_sock.connect((worker_ip, CONFIG_PORT))

            # 2. send data in blocking mode
            model_sock.setblocking(1)

            # send model JSON
            model_json = model_part.to_json()
            socket_send(model_json.encode(), model_sock, self.chunk_size)

            # send worker index
            index_bytes = str(worker_index_val).encode()
            socket_send(index_bytes, model_sock, chunk_size=1)

            # send weights
            self._send_weights(model_part.get_weights(), model_sock, self.chunk_size)

            # 3. set timeout for ACK
            model_sock.settimeout(10)

            # 4. wait for ACK
            # set 60 seconds timeout for select
            ready = select.select([model_sock], [], [], 60)
            if ready[0]:
                try:
                    ack = model_sock.recv(1)
                    if ack == b'\x06':
                        print(f"Worker {worker_ip} acknowledged configuration.")
                    else:
                        # if not ACK, raise error
                        print(f"Worker {worker_ip} sent unexpected ACK: {ack}")
                except socket.timeout:
                    raise socket.timeout("Timed out waiting for Worker ACK (Blocking Read)")
        except Exception as e:
            raise e
        finally:
            model_sock.close()

    """
    Main entry point for DEFER dispatcher
    :param model: The full Keras model to be partitioned and dispatched.
    :param partition_layers: List of layer names where the model should be partitioned.
    :param input_stream: Queue for incoming input data.
    :param output_stream: Queue for outgoing results.
    """
    def run_defer(self, model: tf.keras.Model, partition_layers, input_stream: queue.Queue, output_stream: queue.Queue):

        # 1. start worker monitor thread
        monitor_thread = threading.Thread(target=self._worker_monitor, daemon=True)
        monitor_thread.start()

        # 2. partition model
        self.models_to_dispatch = self._partition(model, partition_layers)

        # 3. wait for initial workers (wait up to 5 seconds)
        max_wait = 5
        for i in range(max_wait):
            initial_workers = self._get_available_workers()
            if initial_workers:
                with self.worker_lock:
                    self.computeNodes = initial_workers
                break
            time.sleep(1)
        else:
            print(f"Error: No workers registered after {max_wait} seconds.")
            # set shutdown event to terminate other threads
            self._shutdown_event.set()
            return

        # 4. start intermediate result server
        intermediate_thread = threading.Thread(target=self._intermediate_result_server, args=(output_stream,),
                                               daemon=True)
        intermediate_thread.start()

        # 5. start task watchdog
        wd_thread = threading.Thread(target=self._task_watchdog, daemon=True)
        wd_thread.start()

        # 6. start distributed edge inference
        start_thread = threading.Thread(target=self._startDistEdgeInference, args=(input_stream,), daemon=True)
        start_thread.start()

        # 7. keep main thread alive
        try:
            while not self._shutdown_event.is_set():
                time.sleep(2)
        except KeyboardInterrupt:
            print("Dispatcher shutting down...")
        finally:
            self._shutdown_event.set()