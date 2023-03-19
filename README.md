Welcome to Tango, a Harmonious Scheduling Framework for K8s-based Edge-cloud Systems with Mixed Services!
==================================

This repository contains the source code for our INFOCOM'23 contributed paper "Tango: Harmonious Scheduling for Mixed Services Co-located among Distributed Edge-clouds".

## Introduction
In order to provide interested readers with an intuitive understanding of the Tango scheduling framework, especially the real system part developed in conjunction with K8s, we have opened source the real system part of the code with further simplification (for just indication). Hope this project can provide or inspire a promising and practical solution for harmonious cloud-edge cluster scheduling with mixed services.


## Environment config
- Ubuntu 18.04 with Linux kernel v5.3.0-28
- python-K8sclient (Python 3.8)
- K8s-v1.19.2
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
