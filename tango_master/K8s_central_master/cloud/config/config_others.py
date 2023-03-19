"""
    About cluster specific information and settings
"""
import random
import logging
from collections import namedtuple


# example: IP of central cluster master
CENTRAL_MASTER_IP = "0.0.0.0"

# example: IP of master and node name of worker in edge cluster
EDGEMASTER_IP_DICTSET = {"master80": {"IP": "192.168.1.80", "nodeset": {"node81", "node82", "node83", "node84"}},
                        "master70": {"IP": "192.168.1.70", "nodeset": {"node71", "node72", "node73", "node74"}},
                        }

# example: Receiving port of the service container
PORT = ['2501', '2500']

# example: Initial resources for the node (used to maintain resource information within Tango)
MEO_TOTAL_INIT = 6192  # M
CPU_TOTAL_INIT = 100000  # 100000 = 1 core

#
SavedAction = namedtuple('SavedAction', ['log_prob', 'value'])
args = namedtuple('args', ('render', 'gamma', 'log_interval'))
args.gamma = 0.97
learning_rate = 2e-4
jitter_setting = 1e-20
GNN_kind = 'graphsage'
Effective_MASTER_FEATURE = 3
Effective_WORKER_FEATURE = 9
FEATURE_SIZE = Effective_MASTER_FEATURE + Effective_WORKER_FEATURE


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
random.seed(0)