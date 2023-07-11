"""
    Initialization process
"""

import os
import logging

os.popen("sudo chmod -R 777 /sys/fs/cgroup/cpu,cpuacct/kubepods/").read()
os.popen("sudo chmod -R 777 /sys/fs/cgroup/memory/kubepods/").read()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

VPA_open_bool = True
logger.info("Worker node complete initialization...")
