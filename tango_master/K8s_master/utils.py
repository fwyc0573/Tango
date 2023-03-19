import threading
import socket
import os
from kubernetes import client, watch, config
import psutil
from checkPod import check_pod
import numpy as np
from config.config_others import EDGEMASTER_IP_DICTSET, NAME


class ReadWriteLock(object):
    def __init__(self):
        self.__monitor = threading.Lock()
        self.__exclude = threading.Lock()
        self.readers = 0

    def acquire_read(self):
        with self.__monitor:
            self.readers += 1
            if self.readers == 1:
                self.__exclude.acquire()

    def release_read(self):
        with self.__monitor:
            self.readers -= 1
            if self.readers == 0:
                self.__exclude.release()

    def acquire_write(self):
        self.__exclude.acquire()

    def release_write(self):
        self.__exclude.release()


def modify_buff_size():
    SEND_BUF_SIZE = 4096 * 10
    RECV_BUF_SIZE = 4096 * 10
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM ) 
    bufsize = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1) 
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUF_SIZE)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECV_BUF_SIZE)
    bufsize = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)


def ping(target_ip, num, size):
    num = str(num)
    size = str(size)
    val = os.popen('ping -c ' + num +  ' -s' + size +  ' '+target_ip).read()
    index_a = val.find('mdev = ',0)
    index_b = val.find('/', index_a) + 1
    average_delay = ""
    while val[index_b] != '/':
        average_delay = average_delay + val[index_b]
        index_b = index_b + 1
    average_delay = float(average_delay)
    return average_delay


def nodes_delay():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ready_nodes = []
    delay = {}
    for n in v1.list_node().items:
        if "master" in n.metadata.name:
            pass
        else:
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ip = n.status.addresses[0].address
                    ready_nodes.append(n)
                    delay[n.metadata.name] = ping(ip, 3, 1000)
                else:
                    pass
    if len(ready_nodes) == 0:
        return -1
    else:
        return delay
    
    
def findSubStrIndex(substr, str, time):
    deploy_type = str.count(substr)
    if (deploy_type == 0) or (deploy_type < time):
        pass
    else:
        i, index = 0, -1
        while i < time:
            index = str.find(substr, index+1)
            i += 1
        return index
    
    
def getPodIDList(pod_list):
    podID_list = []
    for i in pod_list:
        podName = i.name
        # logger.info("i.name:" + str(i.name))  #"imagerecon-deployment1-random1627279122.4124093-7bbcc6b85fmbqn5"
        pos1 = findSubStrIndex("-", podName, 2)
        pos2 = findSubStrIndex("-", podName, 3)
        ID = podName[(pos1+7):pos2]
        podID_list.append(ID)
    return podID_list


def get_master_ip(nodeName):
    for master in EDGEMASTER_IP_DICTSET:
        if nodeName in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
            master_ip = EDGEMASTER_IP_DICTSET[master]["IP"]
            return master_ip
        
        
def get_container_id():
    containerID_list_all = []
    for name in NAME:
        containerID_list_eachSerive = []
        podList = check_pod(name)
        if len(podList) == 0:
            containerID_list_all.append([])
            continue
        for pod in podList:
           podName = pod.name
           command = "kubectl describe pod " + podName + " | grep docker://"
           raw_containerID = os.popen(command).read()
           raw_containerID = raw_containerID.strip()
           pos = raw_containerID.find("/")
           containerID = raw_containerID[pos+2:]
           containerID_list_eachSerive.append(containerID)
        containerID_list_all.append(containerID_list_eachSerive)
    return containerID_list_all


def changBandWidth(bandwidth):
    os.popen('wondershaper ens18 ' + str(bandwidth) + ' ' + str(bandwidth)).read()


def changDelay(delay):
    os.popen('tc qdisc change dev eth18 root netem delay' + str(delay) + ' 10ms').read()


def name_to_ip(master_name):
    ip = "192.168.1."
    for w in master_name:
        if w.isdigit():
            ip += w
    return ip


def distEclud(vecA, vecB):
    return np.sqrt(np.sum(np.power((vecA - vecB), 2)))


def show_memory_info(hint):
    pid = os.getpid()
    p = psutil.Process(pid)
    info = p.memory_full_info()
    memory = info.uss/1024./1024
    print(f"{hint} memory used: {memory} MB ")