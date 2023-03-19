"""
    About cluster specific information and settings
"""
import os

# example: IP of master and node name of worker in edge cluster
EDGEMASTER_IP_DICTSET = {"master80": {"IP": "192.168.1.80", "nodeset": {"node81", "node82", "node83", "node84"}},
                         "master70": {"IP": "192.168.1.70", "nodeset": {"node71", "node72", "node73", "node74"}}, }

MY_EDGE_IP = ""
node_name = os.popen('users').read()
node_name = node_name.split()[0]

for master in EDGEMASTER_IP_DICTSET:
    if node_name in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
        MY_EDGE_IP = EDGEMASTER_IP_DICTSET[master]["IP"]
        break