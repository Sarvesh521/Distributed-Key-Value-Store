# Distributed Fault-Tolerant Key-Value Store

A high-performance, distributed, and fault-tolerant Key-Value store implemented in Java using Spring Boot and gRPC. It employs a **Controller-Worker** architecture with **Consistent Hashing** to ensure scalable data distribution and high availability.

## Key Features

*   **Partitioning**: Uses **Consistent Hashing** (with virtual nodes) to distribute data evenly across worker nodes.
*   **Replication**: Maintains **3 replicas** for every key to ensure durability.
*   **Fault Tolerance**:
    *   **Quorum Writes**: Successful write requires acknowledgement from 2 out of 3 replicas.
    *   **Silent Failover**: Automatically routes requests to healthy replicas if the primary is down.
    *   **Self-Healing**:
        *   **Read Repair**: Fixes stale replicas on-the-fly during read operations.
        *   **Proactive Background Re-replication**: Automatically detects failed nodes and replicates their keys to promoted nodes.
        *   **Delta Sync**: Recovering nodes automatically sync missed data from peers.
*   **Consistency**: Uses **Vector Clocks** for causal consistency and version tracking.
*   **Persistence**: Each worker node is backed by a dedicated **PostgreSQL** database.
*   **Communication**:
    *   **REST API**: For client interactions (Controller).
    *   **gRPC**: For high-performance internal communication (Controller <-> Worker).

## Architecture

The system consists of two main component types:

1.  **Controller (The Brain)**:
    *   Acts as a **Request Proxy** and API Gateway.
    *   Maintains the **Consistent Hash Ring** and registry of active workers.
    *   Handles client requests and routes them to the appropriate primary and replica workers.
    *   Manages failure detection (Heartbeats) and recovery coordination.
    
2.  **Workers (The Storage)**:
    *   Store key-value pairs in a local PostgreSQL database.
    *   Send periodic heartbeats to the Controller.
    *   Handle `PUT`, `GET`, `REPLICATE`, and `SYNC` gRPC requests.

## 🛠 Tech Stack

*   **Language**: Java 17
*   **Framework**: Spring Boot 3.x
*   **Communication**: gRPC (Protobuf), REST
*   **Database**: PostgreSQL 15
*   **Containerization**: Docker & Docker Compose
*   **Build Tool**: Maven

## Getting Started

### Prerequisites
*   Docker Desktop installed and running.
*   Java 17+ (optional, for local development).

### Running the Cluster
The entire cluster (1 Controller, 4 Workers, 4 Databases) is orchestrated via Docker Compose.

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd Distributed-Key-Value-Store
    ```

2.  **Build and Start**:
    ```bash
    docker-compose up --build -d
    ```

3.  **Verify**:
    Check if all containers are healthy:
    ```bash
    docker ps
    ```
    You should see `kv-controller`, `w1`, `w2`, `w3`, `w4`, and their respective databases.

## 🔌 API Reference

Base URL: `http://localhost:8080`

### Client Endpoints

#### 1. Store a Value (PUT)
Stores a key-value pair. The system ensures it is replicated to a quorum.

*   **Endpoint**: `POST /api/kv/{key}`
*   **Body**: Text (The value)
*   **Example**:
    ```bash
    curl -X POST -H "Content-Type: text/plain" -d "Hello Distributed World" http://localhost:8080/api/kv/my-key
    ```
*   **Response**: `200 OK` - "Stored successfully. {Synchronous Replicas: [w1, w2]}, {Asynchronous Replica: w3}"

#### 2. Retrieve a Value (GET)
Retrieves the latest version of a value. Performs Read Repair if inconsistences are found.

*   **Endpoint**: `GET /api/kv/{key}`
*   **Example**:
    ```bash
    curl http://localhost:8080/api/kv/my-key
    ```
*   **Response**: `200 OK` - "Value: Hello Distributed World (Source: w1)"

### Admin / Debug Endpoints

#### 3. List Active Workers
Shows the list of workers currently registered in the ring.

*   **Endpoint**: `GET /api/kv/workers`

#### 4. Inspect Worker Data
Retrieves all key-value pairs stored on a specific worker node.

*   **Endpoint**: `GET /api/kv/worker/{workerId}`
*   **Example**:
    ```bash
    curl http://localhost:8080/api/kv/worker/w1
    ```

## Testing Fault Tolerance

You can simulate failures to see the system's self-healing capabilities in action.

**Scenario: Worker Failure & Auto-Recovery**

1.  **Write Data**:
    ```bash
    curl -X POST -d "Persistent Data" http://localhost:8080/api/kv/safe-key
    ```
2.  **Kill a Worker** (e.g., the primary for that key):
    ```bash
    docker stop distributed-key-value-store-w1-1
    ```
3.  **Read Data**:
    ```bash
    curl http://localhost:8080/api/kv/safe-key
    ```
    *Result*: The request succeeds! The controller automatically routes to the surviving replicas.
4.  **Verify Re-replication**:
    Wait ~6 seconds (heartbeat timeout). The controller logs will show "Starting proactive re-replication".
    Check a previously uninvolved worker (e.g., `w4`) to see if it received the data:
    ```bash
    curl http://localhost:8080/api/kv/worker/w4
    ```

## Project Structure

```
.
├── kv-common       # Shared library (Protobufs, Hashing logic)
├── kv-controller   # Controller service implementation
├── kv-worker       # Worker service implementation
└── docker-compose.yml
```
