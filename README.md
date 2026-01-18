# ADAPT Distributed Inference

ADAPT splits a TensorFlow model across many machines. A dispatcher sends each worker the correct model slice, forwards data through the chain, and collects the final result. Use this repo when you want ResNet-50 to keep running even if workers come and go.

---

## What Is Inside

- [src/dispatcher.py](src/dispatcher.py) holds the dispatcher (`DEFER` class).
- [src/node.py](src/node.py) starts a worker that receives weights, runs inference, and forwards results.
- [src/node_state.py](src/node_state.py) defines socket helpers and the etcd-backed worker state.
- [test/test.py](test/test.py) is the sample driver that feeds images into the system.

---

## Requirements

- Python 3.10 or newer.
- TensorFlow 2.15, numpy, psutil, pynvml, etcd3, lz4, zfpy, pillow.
- An etcd v3 server reachable from dispatcher and workers (script: `src/start_etcd.sh`).
- Ports 6000-6003 open between dispatcher and every worker.

Install packages after cloning:

```bash
python -m venv .venv
source .venv/bin/activate   # Use .\.venv\Scripts\Activate.ps1 on Windows
python -m pip install --upgrade pip
python -m pip install tensorflow==2.15.0 numpy psutil pynvml etcd3 lz4 zfpy pillow
```

Place a test image at `resource/test.jpg` before running the demo.

---

## How to Run

1. **Start etcd** on the dispatcher machine.
   - Linux/WSL: `bash src/start_etcd.sh`
   - Windows: download etcd zip, run `etcd.exe --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://<dispatcher-ip>:2379`

2. **Start workers** on each compute node.
   - Edit [src/node.py](src/node.py) if you need to change dispatcher IP or chunk size.
   - Run `python -m src.node` (keep one process per worker machine or port range).

3. **Configure the dispatcher** in [test/test.py](test/test.py).
   - Set `computeNodes` to the worker IP list.
   - Populate `part_at` with layer names if you want more than one partition (leave empty for single chunk).

4. **Launch the pipeline** from the repository root:

   ```bash
   python test/test.py
   ```

   The script loads ResNet-50, partitions it, sends weights to workers, and prints throughput once `task_size` batches finish.

5. **Send your own inputs** by writing tensors into the `input_q` queue inside `test/test.py` instead of the sample loop.

---
