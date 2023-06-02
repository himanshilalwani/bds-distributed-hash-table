import random, faker
from pymemcache.client.base import Client
from pymemcache.client.murmur3 import murmur3_32
import copy

# function to find the 
def insert_sorted(arr, x):
    left, right = 0, len(arr) - 1
    while left <= right:
        mid = (left + right) // 2
        if arr[mid] < x:
            left = mid + 1
        else:
            right = mid - 1
    return left

class Node(object):
    # Class representing a memcached node
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.server = Client((ip, port)) # connection to memcached
    def __str__(self):
        return f"{self.ip}:{self.port}"
# 
class ConsistentHashRing(object):
    # Class implementing a consistent hash ring for distributed hashing
    def __init__(self, replication_factor = 2):
        self.replication_factor = replication_factor
        self.ring = [] # ring with virtual nodes
        self.nodes = {} # hashed key to node map

    def add_node(self, node):
        # Adds a new node to the hash ring with one virtual node
        for i in range(self.replication_factor):
            key = self.get_hashed_key(node, i)
            # print("node hash: ", str(node), key)
            self.ring.append(key)
            self.nodes[key] = node
        self.ring.sort()

    def remove_node(self, node):
         # Removes a node from the hash ring with its virtual node
        for i in range(self.replication_factor):
            key = self.get_hashed_key(node, i)
            del self.nodes[key]
            self.ring.remove(key)

    def get_hashed_key(self, node, i):
        # Generates a hashed key using the MurmurHash algorithm
        return murmur3_32(f"{node}-{i}")

    def get_node(self, key):
        # Returns the node responsible for the given key
        hashed_key = murmur3_32(str(key))
        idx = insert_sorted(self.ring, hashed_key) # finds the next server on the ring
        if idx == len(self.ring): # if index = end of list, server = first one
            return self.nodes[self.ring[0]]
        return self.nodes[self.ring[idx]]

class DistributedHashTable(object):
    # Class implementing a distributed hash table using a consistent hash ring
    def __init__(self, nodes):
        self.ring = ConsistentHashRing()
        self.nodes = {} # Dictionary mapping string representation of a node to Node object
        self.nodes_keys = {} # Dictionary mapping string representation of a node to a list of keys
        # adding the initial servers
        for node in nodes:
            self.add_node(node)
        # finding the servers to replicate the keys to for a given server
        self.replicate()
    
    # Given a node in the hash ring, returns the next two unique nodes in the ring
    def find_next_two(self, curr):
        idx = self.sorted_list.index(str(curr))
        if (idx == len(self.sorted_list) - 1):
            return self.sorted_list[0], self.sorted_list[1]
        elif (idx == len(self.sorted_list) - 2):
            return self.sorted_list[-1], self.sorted_list[0]
        else:
            return self.sorted_list[idx+1], self.sorted_list[idx+2]

    # Replicates keys across the hash ring to ensure fault tolerance
    def replicate(self):
        self.replication_nodes = {}
        for node in self.nodes:
            newNode = str(self.nodes[node])
            self.replication_nodes[newNode] = set()
        # sorting the ring
        sorted_dict = dict(sorted(self.ring.nodes.items()))
        for key in sorted_dict:
            sorted_dict[key] = str(self.nodes[sorted_dict[key]])
        self.sorted_list = []
        # creating a sorted list for simplification
        for key in sorted_dict:
            if sorted_dict[key] not in self.sorted_list:
                self.sorted_list.append(sorted_dict[key]) 
        # for each server, finding the next two servers on the ring and adding to the replication_nodes mapping
        for hash_val in self.ring.ring:
            curr_node = self.nodes[self.ring.nodes[hash_val]]
            server1, server2 = self.find_next_two(curr_node)
            self.replication_nodes[str(curr_node)].add(server1)
            self.replication_nodes[str(curr_node)].add(server2)

    def add_node(self, node):
        self.nodes[str(node)] = Node(node[0], node[1])
        self.ring.add_node(str(node))
        # if not initial servers
        if len(self.nodes_keys) != 0:
            self.replicate()
            for i in range(self.ring.replication_factor):
                idx = insert_sorted(self.ring.ring,  self.ring.get_hashed_key(node, i))
                upper_bound_server = self.nodes[self.ring.nodes[self.ring.ring[idx+1]]] # server right after the current server on the ring
                if str(upper_bound_server) in self.nodes_keys:
                    # finding the keys to be reassigned
                    reassignment_list = self.nodes_keys[str(upper_bound_server)]
                    for pair in reassignment_list:
                        n = self.get_node(pair[0]) # node where the key will be reassigned
                        # if node is not the upper bound server, the key will be added to the new server
                        if n != upper_bound_server:
                            for p in range(len(self.nodes_keys[str(upper_bound_server)])):
                                if self.nodes_keys[str(upper_bound_server)][p] == pair:
                                    # deleting the key from the upper bound server
                                    del self.nodes_keys[str(upper_bound_server)][p]
                                    if str(n) not in self.nodes_keys:
                                        self.nodes_keys[str(n)] = list()
                                    self.nodes_keys[str(n)].append((str(pair[0]), pair[1]))
                                    # adding it to the new server
                                    n.server.set(pair[0], pair[1])
                                    upper_bound_server.server.delete(pair[0])
                                    # deleting data from the replication servers
                                    for replica in self.replication_nodes[str(upper_bound_server)]:
                                        for nd in self.nodes:
                                            if str(self.nodes[nd]) == replica:
                                                self.nodes[nd].server.delete(pair[0])
                                                break
                                    # adding data to the replication servers
                                    for replica in self.replication_nodes[str(n)]:
                                        for nd in self.nodes:
                                            if str(self.nodes[nd]) == replica:
                                                self.nodes[nd].server.set(pair[0], pair[1])
                                                break
                                    break

    def remove_node(self, node):
        # removing the given node from the ring
        self.ring.remove_node(str(node))
        del self.nodes[str(node)]
        # reassigning the keys stored on that server
        n = node[0] + ":" + str(node[1])
        reassignment_list = self.nodes_keys[n]
        for pair in reassignment_list:
           self.set(pair[0], pair[1])
        del self.nodes_keys[n]
   
    def get_node(self, key):
        node = self.ring.get_node(key)
        return self.nodes[node]

    # setting a key and a value
    def set(self, key, value):
        node = self.get_node(key)
        if str(node) not in self.nodes_keys:
            self.nodes_keys[str(node)] = list()
        self.nodes_keys[str(node)].append((str(key), value))

        # adding key-value to the replication servers
        for replica in self.replication_nodes[str(node)]:
            for n in self.nodes:
                if str(self.nodes[n]) == replica:
                    self.nodes[n].server.set(str(key), value)
                    break
        return node.server.set(str(key), value)
	
    # getting the value for the given key
    def get(self, key):
        node = self.get_node(key)
        if node is None:
            return None
        val =  node.server.get(str(key))
        if val != None:
            return val.decode()
        else:
            # looking for the value in the replication servers
            for replica in self.replication_nodes[str(node)]:
                for n in self.nodes:
                    if str(self.nodes[n]) == replica:
                        val = self.nodes[n].server.get(str(key))
                        if val != None:
                            return val.decode()
                        
        return None 

# helper function
def read_list_func(rlist, dht):
    output = {}
    for key in rlist:
        value = dht.get(key)
        # print(f"Key: {key}, Value: {value}")
        output[key] = value
    return output

print("My Memcached DHT.")
print("Initializing the DHT Cluster:")
nodes = [("localhost", 11211), ("localhost", 11212), ("localhost", 11213), ("localhost", 11214)]
dht = DistributedHashTable(nodes)

# Write 100 key-value pairs to Memcached (you add more if you want)
print("Loading some fake data")
fake = faker.Faker()
for key in range(100):
    value = fake.company()
    dht.set(key, value)

### TEST
# Sample 10 random keys to read from Memcached to test the system
read_list = random.sample(range(100), 50)

print("My Memcache DHT")
# Check the status of the value
o1 = read_list_func(read_list, dht) # Check the content of the cache
o2 = dict()
o3 = dict()
# Simulating the failure of a node m1
m1 = ("localhost", 11211)
if (str(m1) in dht.nodes):
    dht.remove_node(m1)
    o2 = read_list_func(read_list, dht) # Check the content of the cache
else:
    print("Node not found")

# Simulating the addition of a new node m5
try:
    dht.add_node(('localhost', 11215))
    o3 = read_list_func(read_list, dht) # Check the content of the cache
except:
    print("Connection error")

# automating testing by comparing the 3 dictionaries
if (o1 == o2 and o2 == o3 and o3 == o1):
    print("Test Passed")
else:
    print("Outputs Don't Match")

# interactive shell
while True:
    try:
        user_input = input("1. get <key>\n2. set <key> <value>\n3. add <node_ip> <node_port>\n4. remove <node_ip> <node_port>\n5. quit\nEnter a command: ")
        if user_input == "quit":
            break
        user_input = user_input.split(" ")
        if user_input[0] == "get":
            k = user_input[1]
            value = dht.get(k)
            if value == None:
                print("Key Not Found")
            else:
                print(f"Key: {k}, Value: {value}")
        elif user_input[0] == "set":
            k = user_input[1]
            v = user_input[2]
            dht.set(k, v)
            print("Done!")
        elif user_input[0] == "add":
            ip = user_input[1]
            port = user_input[2]
            dht.add_node((ip, port))
            print("Added Node")
        elif user_input[0] == "remove":
            ip = user_input[1]
            port = int(user_input[2])
            newNode = str((ip, port))
            if (newNode in dht.nodes):
                dht.remove_node((ip, port))
                print("Removed Node")
            else:
                print("Node not found")
        else:
            print("Incorrect Command")
    except:
        print("Incorrect Command / Params")

