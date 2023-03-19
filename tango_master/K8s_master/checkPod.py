# -*- coding: utf-8 -*- 
import os
import re
from dataclasses import dataclass
import time

@dataclass
class Pod(object):
	name: str
	ready: str
	status: str
	restarts: str
	age: str
	ip: str
	node: str
	containerID:str


def check_pod(target, target_node = "", defaultFindRuning = True):
	index = 0
	index_list = []

	grep_get_pod_str = 'kubectl get pod -o wide | grep ' + target
	pods = os.popen(grep_get_pod_str).read()

	while pods.find(target,index) != -1:
		if pods.find(target,index) != len(pods)-1:
			index_list.append(pods.find(target,index))
			index = pods.find(target,index) + 1
		else:
			index = pods.find(target,index)
			break

	pod_list = []
	for i in range(len(index_list)):
		target_pod = Pod("","","","","","","","")
		index = index_list[i]
		while pods[index] != " ":
			target_pod.name = target_pod.name + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		while pods[index] != " ":
			target_pod.ready = target_pod.ready + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		while pods[index] != " ":
			target_pod.status = target_pod.status + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		if defaultFindRuning:
			if target_pod.status != "Running":
				continue

		while pods[index] != " ":
			target_pod.restarts = target_pod.restarts + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		while pods[index] != " ":
			target_pod.age = target_pod.age + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		while pods[index] != " ":
			target_pod.ip = target_pod.ip + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1
	
		while pods[index] != " ":
			target_pod.node = target_pod.node + pods[index]
			index = index + 1
		while pods[index] == " ":
			index = index + 1

		if target_node:
			if target_pod.node != target_node:
				continue

		grep_get_containerID_str = 'kubectl describe pod ' + target_pod.name + '| grep "Container ID:"'
		grep_get_containerID_str = os.popen(grep_get_containerID_str).read()
		pos = grep_get_containerID_str.find("/")
		target_pod.containerID = grep_get_containerID_str[pos+2:].strip()

		pod_list.append(target_pod)
	return pod_list



if __name__ == '__main__':
	t_s = time.time()
	# podList = check_pod("service1", "node82")
	podList = check_pod("service1")
	for pod in podList:
		print(pod.ip)
	# print(len(check_pod("service1")))
	# print(check_pod("service1"))
	# print(podList)