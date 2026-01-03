# ADAPT: Adaptive Deep-learning Architecture for Parallel and Tolerant Inference

ADAPT is a **fault-tolerant, adaptive distributed inference framework** designed for running deep neural network (DNN) inference across **heterogeneous and failure-prone edge devices**. It rethinks pipeline-parallel inference by replacing rigid peer-to-peer chains (e.g., DEFER) with a **centralized dispatcher**, **dynamic worker scheduling**, and **multi-layer fault tolerance**.

This project was developed as part of **ECE 750 – Scalable Computer System Design** and evaluated using **ResNet-50 inference** over mixed CPU/GPU workers.

---

## 🚀 Key Features

* **Centralized Dispatcher Architecture**

  * Eliminates brittle worker-to-worker pipelines
  * Enables global visibility, scheduling, and recovery

* **Dynamic, Resource-Aware Scheduling**

  * Workers selected based on real-time memory usage
  * Lowest-load-first policy with cooldown

* **Dual-Layer Fault Tolerance**

  * *Soft failures*: timeout-based task retries
  * *Hard failures*: heartbeat-based detection via Etcd

* **QoS-Based Reliability Scoring**

  * Penalizes unstable workers
  * Circuit breaker for repeatedly failing nodes

* **Elastic Worker Membership**

  * Workers can join/leave at runtime
  * Stateless execution model

* **Pipeline Parallelism**

  * ResNet-50 partitioned across workers
  * Supports configurable pipeline concurrency

* **Transparent CPU/GPU Execution**

  * GPU used when available, CPU otherwise

---

## 🧠 Motivation

Existing distributed edge inference systems such as **DEFER** assume:

* Stable workers
* Fixed pipelines
* No runtime failures
* No system observability

These assumptions break down in real-world edge environments where:

* Nodes crash or disconnect
* Resources fluctuate
* Hardware is heterogeneous

**ADAPT** addresses these challenges by prioritizing **robustness, adaptability, and observability**, even at the cost of slight performance overhead.

---

## 🏗️ System Architecture

```
        +-------------------+
        |     Dispatcher    |
        |-------------------|
        | Scheduling        |
        | Fault Handling    |
        | QoS Scoring       |
        +---------+---------+
                  |
      ---------------------------------
      |        |        |        |     |
 +---------+ +---------+ +---------+  |
 | Worker  | | Worker  | | Worker  | ...
 | (CPU)   | | (GPU)   | | (CPU)   |
 +---------+ +---------+ +---------+
                  |
              +--------+
              |  Etcd  |
              |--------|
              | State  |
              | Health |
              +--------+
```

* **Dispatcher**: Assigns tasks, handles retries, manages QoS
* **Workers**: Stateless executors of model partitions
* **Etcd**: Global state store (heartbeats, memory usage, liveness)

---

## ⚙️ Methodology Overview

### Model Partitioning

* ResNet-50 split into sequential partitions
* Each partition executed independently

### Scheduling Policy

* Select worker with **lowest memory usage + penalty score**

### QoS Penalty Rules

* Task failure: `S_penalty += 50`
* Task success: `S_penalty *= 0.8`
* Circuit breaker at `S_penalty > 200`

### Fault Handling

* **Soft Failures**: Timeout → retry on new worker
* **Hard Failures**: Missed Etcd heartbeat → worker removed

---

## 📊 Experimental Evaluation

### Setup

* Model: **ResNet-50**
* Dataset: **Food-101** (batch size = 1)
* Workers: 1, 4, and 8
* Mixed CPU/GPU environment

### Results Summary

| Metric                    | Observation               |
| ------------------------- | ------------------------- |
| Best Pipeline Concurrency | 4                         |
| Latency Reduction         | ~75% (vs concurrency=1)   |
| Worker Utilization        | >80% balanced             |
| Fault Recovery            | Automatic, no system halt |

ADAPT maintains stable throughput even under worker churn, unlike DEFER where a single failure collapses the pipeline.

---

## ⚖️ ADAPT vs DEFER

| Aspect          | DEFER               | ADAPT                     |
| --------------- | ------------------- | ------------------------- |
| Topology        | Fixed pipeline      | Central dispatcher        |
| Fault Tolerance | ❌ None              | ✅ Dual-layer              |
| Worker Churn    | ❌ Unsupported       | ✅ Supported               |
| Scheduling      | Static              | Dynamic, load-aware       |
| Observability   | ❌ None              | ✅ Etcd-backed             |
| Performance     | Higher (ideal case) | Slightly lower but robust |

---

## ⚠️ Limitations

* Dispatcher is a potential bottleneck
* Stateless workers increase network I/O
* Memory-only scheduling ignores CPU thermal throttling

---

## 🔮 Future Work

* Multi-dispatcher architecture
* NPU / accelerator-aware scheduling
* Model parameter caching
* Partition-level replication

---

## 📚 References

* Parthasarathy et al., *DEFER: Distributed Edge Inference*, 2022
* Zeng et al., *CoEdge*, IEEE/ACM ToN, 2020

---

## 👥 Authors

* Xuanzheng Zhang
* Bryan Ronnie Jayasingh
* Madhav Srinath Thanigaivel
* Barun Gnanasekaran
* Karthi Elayaperumal Sampath

---

## 📄 License

This project is released for **academic and research purposes**.
