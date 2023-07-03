Welcome to Tango, a Harmonious Management and Scheduling Framework for K8s-based Edge-Cloud Systems with Mixed Services!
==================================

## Introduction
Tango is a management and scheduling framework designed to achieve harmonious scheduling in cloud-edge clusters with mixed services. This repository contains the source code of the real system developed in conjunction with K8s, providing a simplified version for demonstration purposes. We hope this project can serve as a practical and promising solution for efficient cloud-edge cluster management and scheduling.

## Environment config
- Ubuntu 20.04.3LTS with Linux kernel v5.3.0-28
- python-K8sclient (Python 3.8)
- K8s-v1.21.0
- Pytorch 1.11.0

To install the dependencies, please execute the following command on the master node:

```
pip3 install -r requirements_master.txt
```

And execute the following command on the central master node:

```
pip3 install -r requirements_central_master.txt
```

## Project
- tango_master
    - K8s_cental_master: includes the operation and component implementation of cental edge clusters.
    - K8s_master: includes edge cluster operation and component implementation.
- tango_request_generator
    - includes operation of the request generator where the real workloaod traces used are from Google Borg: https://github.com/google/cluster-data.
- tango_service
    - a setting example for mixed service YAML file.
- tango_worker
    - the main script used to implemente the components and operations of the worker node.
    
## Getting started

- Trace: You can download the required data from the above Google trace.
- Config setting: Please complete the container and YAML file first and set up in config file according to your own needs. We have given some examples of settings.
- Run in the following order:  
    - worker node -> `worker_run.py`
    - central master node -> `main_central.py`
    - master node -> `master_run.py` and `init_deploy_lc.py`
    - req_generator -> `reqGenerator_run.py`
- Observe: We have provided some observation interfaces in the main script.

## Version
- v0.1 beta
