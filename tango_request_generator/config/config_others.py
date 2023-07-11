"""
    About cluster specific information and settings
"""

import logging
import random

request_path = "random_my_req.csv"
CENTRAL_MASTER_IP = "192.168.1.75"

# IP INFO
EDGEMASTER_IP_DICTSET = {
    "master80": {
        "IP": "192.168.1.80",
        "nodeset": {"node81", "node82", "node83", "node84"},
    },
    "master70": {
        "IP": "192.168.1.70",
        "nodeset": {"node71", "node72", "node73", "node74"},
    },
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

random.seed(0)
