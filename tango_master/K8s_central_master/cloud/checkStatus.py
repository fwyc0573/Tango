# -*- coding: utf-8 -*- 
import os
import time
from kubernetes import client, watch, config
import json
import re
import pandas as pd
from io import StringIO
import subprocess
# from check_pod import check_pod


# /proc/cpuinfo
def cpuinfo():
    info = os.popen('cat /proc/cpuinfo').read().split('\n')
    result = {}
    for i in range(len(info)):
        if info[i].find('\t: ') == -1:
            continue
        result[info[i].split('\t: ')[0]] = info[i].split('\t: ')[1]
    return result


# /proc/meminfo
def meminfo():
    info = os.popen('cat /proc/meminfo').read().split('\n')
    result = {}
    for i in range(len(info)):
        if info[i].find(':        ') == -1:
            continue
        result[info[i].split(':        ')[0]] = info[i].split(':        ')[1].strip()
    return result


# /proc/uptime
def uptime():
    info = os.popen('cat /proc/uptime').read().split(' ')
    result = {}
    result['run time'] = info[0].strip()
    result['idle time'] = info[1].strip()
    return result


# pending pod
def pod():
    config.load_kube_config()
    v1 = client.CoreV1Api()

    pending_pod = [x for x in v1.list_pod_for_all_namespaces(watch=False).items]
    # print(pending_pod)
    return str(pending_pod)


def getNodeName():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    nodeList = []
    
    for n in v1.list_node().items:
        if "node" in n.metadata.name:
            nodeName = n.metadata.name
            nodeList.append(nodeName)
    return nodeList
    
            
# all ready nodes
def nodes():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    ready_nodes = []
    resource = {}
    
    master_name = ''
    for n in v1.list_node().items:
        if "master" in n.metadata.name:
            master_name = n.metadata.name
            resource = {master_name: {}}
            # print(n.status.conditions)
        else:
            # print("11111")
            for status in n.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    ready_nodes.append(n)
                    current_resource = {}
                    record_data_current_resource = {}
                    # print(n.metadata.name)
                    # total_str = os.popen('kubectl describe node ' + n.metadata.name).read()
                    # node_info = pd.read_csv(StringIO(subprocess.getoutput(
                                            # 'kubectl top nodes '+ n.metadata.name)),sep='\s{2,}')
                    # for index,row in node_info.iterrows()
                        # cpu_number = row[1][:-1]#166m
                        # cpu_percent = row[2][:-1]#4%
                        # memory_number = row[3][:-2]#1646Mi
                        # memory_percent = row[4][:-1]#8%
                    # current_resource['memory'] = n.status.allocatable['memory']
                    # current_resource['ephemeral-storage'] = n.status.allocatable['ephemeral-storage']

                    total_str = os.popen('kubectl describe node ' + n.metadata.name).read()
                    total = re.split('\n', total_str)
                    record_line_start = 0
                    for index_line in range(len(total)):
                        if "Allocated resources:" in total[index_line]:
                            record_line_start = index_line
                            break
                    memory = ' '.join(total[record_line_start+5].split())
                    # storage = ' '.join(total[-5].split())
                    try:
                        memory_percent = memory.split(' ')[-3][1:-2]
                        memory_number = memory.split(' ')[-4][0:-2]
                        # storage_percent = storage.split(' ')[-1][1:-2]
                        # storage_number = storage.split(' ')[-2]
                        current_resource['memory'] = {'percent': memory_percent, 'number': memory_number}
                        # current_resource['storage'] = {'percent': storage_percent, 'number': storage_number}
                        resource[master_name][n.metadata.name] = current_resource
                        record_data_current_resource = current_resource
                    except Exception as e:
                        print("checkstatus nodes() error::", e, "BUT FIX TITH REOCRD DATA ")
                        print("memory:", memory)
                        resource[master_name][n.metadata.name] = record_data_current_resource
                else:
                    pass
    if len(ready_nodes) == 0:
        return -1
    else:
        return resource


def check_status():
    result = []
    with open('./json/cpu_' + str(time.time()) + '.json', 'w') as file_obj:
        cpu = cpuinfo()
        json.dump(cpu, file_obj)
        result.append(cpu)

    with open('./json/mem_' + str(time.time()) + '.json', 'w') as file_obj:
        mem = meminfo()
        json.dump(mem, file_obj)
        result.append(mem)

    with open('./json/uptime_' + str(time.time()) + '.json', 'w') as file_obj:
        up_time = uptime()
        json.dump(up_time, file_obj)
        result.append(up_time)

    with open('./json/pod_' + str(time.time()) + '.json', 'w') as file_obj:
        pending_pod = pod()
        json.dump(pending_pod, file_obj)
        result.append(pending_pod)

    with open('./json/nodes_' + str(time.time()) + '.json', 'w') as file_obj:
        ready_nodes = nodes()
        json.dump(ready_nodes, file_obj)
        result.append(ready_nodes)

    return result


if __name__ == '__main__':
    resource = nodes()
    print(resource)
    # while True:
    #     with open('./json/cpu_' + str(time.time()) + '.json', 'w') as file_obj:
    #         cpu = cpuinfo()
    #         json.dump(cpu, file_obj)
    #
    #     with open('./json/mem_' + str(time.time()) + '.json', 'w') as file_obj:
    #         mem = meminfo()
    #         json.dump(mem, file_obj)
    #
    #     with open('./json/uptime_' + str(time.time()) + '.json', 'w') as file_obj:
    #         uptime = uptime()
    #         json.dump(uptime, file_obj)
    #
    #     with open('./json/pod_' + str(time.time()) + '.json', 'w') as file_obj:
    #         pod = pod()
    #         json.dump(pod, file_obj)
    #
    #     with open('./json/nodes_' + str(time.time()) + '.json', 'w') as file_obj:
    #         nodes = nodes()
    #         json.dump(nodes, file_obj)
    #
    #     interval = 5000
    #     time.sleep(interval)
