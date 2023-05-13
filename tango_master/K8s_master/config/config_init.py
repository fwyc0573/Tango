"""
    Initialization process
"""

import logging
import random
import time
import os
import pandas as pd
from io import StringIO
import subprocess
import sys

sys.path.append("..")
from deploy import (
    delete,
    deploy,
    deploy_with_node,
    delete_anyway,
    init_BE,
    initDeleteAll,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Clear all service containers from the last deployment
initDeleteAll()
logger.info("All containers last allowed have been cleared...")

BE_CONTAINER_IP_DICT = init_BE()
logger.info("BE containers have been successfully created...")

# os.popen("python3 init_deploy_lc.py").read()
# logger.info("LC containers have been successfully created...")

random.seed(0)
t_sys = time.time()


def fetch_edge_master_name():
    nodes_list = pd.read_csv(
        StringIO(subprocess.getoutput("kubectl get node -o wide")),
        sep="\s{2,}",
        engine="python",
    )
    master_name = nodes_list[nodes_list["ROLES"] == "master"]["NAME"].iloc[0]
    return master_name


master_name = fetch_edge_master_name()
