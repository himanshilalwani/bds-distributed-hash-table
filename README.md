# Distributed Hash Table with Consistent Hash Ring
This project is an implementation of a distributed hash table (DHT) using a consistent hash ring. The DHT is distributed across multiple servers, and the hash ring is used to map keys to the servers in the DHT. The hash ring's consistent nature ensures that adding or removing a node from the ring causes minimal disruption to the key-value assignments, resulting in efficient and balanced distribution of data across the DHT.

## Instructions
- Open a terminal or command prompt.
- Navigate to the directory where dht_starter.py is located.
- Run the code by executing the following command: `python3 dht_starter.py`

## Usage
Once the code is running, you can use the interactive shell to interact with the DHT. The following commands are available:
- `put <key> <value>`: Store a key-value pair in the DHT.
- `get <key>`: Retrieve the value associated with a key from the DHT.
- `add_node <ip> <port>`: Add a new node to the DHT.
- `remove_node <ip> <port>`: Remove a node from the DHT.

## Dependencies
The code relies on the following dependencies: `random`, `faker`, and `pymemcache`. Please ensure that these dependencies are installed before running the code.
