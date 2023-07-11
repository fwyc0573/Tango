Welcome to Tango, a Harmonious Management and Scheduling Framework for K8s-based Edge-Cloud Systems with Mixed Services!
==================================

## Introduction
Yicheng Feng, Shihao Shen, Mengwei Xu, Yuanming Ren, Xiaofei Wang, Victor C.M. Leung, and Wenyu Wang, "Tango: Harmonious Management and Scheduling for Mixed Services Co-located among Distributed Edge-Clouds," in **ACM International Conference on Parallel Processing (ICPP)**, 2023.

Tango is a management and scheduling framework designed to achieve harmonious scheduling in cloud-edge clusters with mixed services. This repository contains the source code of the real system developed in conjunction with K8s, providing a simplified version for demonstration purposes. We hope this project can serve as a practical and promising solution for efficient cloud-edge cluster management and scheduling.

## Environment Config
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
    - K8s_cental_master: Includes the operation and component implementation of central edge clusters.
    - K8s_master:  Includes the operation and component implementation of edge clusters.
- tango_request_generator
    - Includes the operation of the request generator. The real workload traces used are from Google Borg: https://github.com/google/cluster-data.
- tango_service
    - An example setting for a mixed service YAML file (leveraging k8s-QoS-level).
- tango_worker
    - The main script used to implement the components and operations of the worker node.
    

## Getting started

- Trace: You can download the required data from the above Google trace.
- Config setting:  Please complete the container and YAML file first, and set them up in the config file according to your own needs. We have provided some examples of settings.
- Run in the following order:  
    - worker node -> `worker_run.py`
    - central master node -> `main_central.py`
    - master node -> `master_run.py` and `init_deploy_lc.py`
    - req_generator -> `reqGenerator_run.py`
- Observe: We have provided some observation interfaces in the main script.



## Notes
- Tango architecture is developed for supporting cri-dockerd-based K8S clusters. As the container runtime for K8S is 
transitioning to containerd in the near future, we are actively expanding Tango to support a wider range of scenarios.

- In the containerized scenarios of the PPIO Edge Cloud Enterprise, more application containers are deployed in a stable
manner on servers (without frequent active migration or horizontal scaling). Therefore, Tango maintains the traffic 
load of containerized applications based on the deployment level (the architecture maintains the Pod's ID). 
We are expanding Tango to enable better implementation of traffic load at the service level.



## Version
- v0.1 beta


## Citation
If this paper can benefit your scientific publications, please kindly cite it.

```
Yicheng Feng, Shihao Shen, Mengwei Xu, Yuanming Ren, Xiaofei Wang, Victor C.M. Leung, and Wenyu Wang, "Tango: Harmonious Management and Scheduling for Mixed Services Co-located among Distributed Edge-Clouds," in ACM International Conference on Parallel Processing (ICPP), 2023.
```
