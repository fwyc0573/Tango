"""
    About cluster specific information and settings
"""

CONTAINER_RECORD_LIST = []
k8s_rr_index_lc = 0
k8s_rr_index_be = 0
delay_user_cloud_simple = 0
delay_user_edge_simple = 0

node_position = {"master80": [1, 1], "node81": [1, 1], "node82": [1, 1], "node83": [1, 1], "node84": [1, 1],
                  "master70": [2, 2], "node71": [2, 2], "node72": [2, 2], "node73": [2, 2], "node74": [2, 2],
                 }
node_trans_property = {}
for node in node_position:
    node_trans_property[node] = 0

# example: IP of central cluster master
CENTRAL_MASTER_IP = "192.168.1.75"

# example: IP of master and node name of worker in edge cluster
EDGEMASTER_IP_DICTSET = {"master80": {"IP": "192.168.1.80", "nodeset": {"node81", "node82", "node83", "node84"}},
                        "master70": {"IP": "192.168.1.70", "nodeset": {"node71", "node72", "node73", "node74"}},
                        }

# example: Initial resources for the node (used to maintain resource information within Tango)
MEO_TOTAL_INIT = 6192  # M
CPU_TOTAL_INIT = 100000  # 100000 = 1 core

# example: Service Name
NAME = ['service1', 'service0', ]
# example: Path to the service YAML file
PATH = ['./app/lc1/service.yaml', './app/be/service.yaml']
# example: Receiving port of the service container
PORT = ['2501', '2500']