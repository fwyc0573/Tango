import yaml
from kubernetes import client, config, watch
from ruamel import yaml
import time
import os
from checkPod import check_pod
from checkStatus import getNodeName
import shutil
import logging
from config.config_others import NAME, PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def bind_name(v1, pod, node, namespace="default"):
    target = client.V1ObjectReference(api_version='v1', kind='Node', name=node)
    meta = client.V1ObjectMeta()
    meta.name = pod
    body = client.V1Binding(target=target, metadata=meta)
    try:
        print("INFO Pod: %s placed on: %s\n" % (pod, node))
        print(pod, ' choose node ', node)
        api_response = v1.create_namespaced_pod_binding(
            name=pod, namespace=namespace, body=body)
        print(api_response)
        return api_response
    except Exception as e:
        print("Warning when calling CoreV1Api->create_namespaced_pod_binding: %s\n" % e)


def yamlCreat(str_node_index, deploy_type):
    t1 = str(time.time())
    file_path = "/home/" + str(t1) + ".yaml"
    shutil.copyfile(PATH[deploy_type-1], file_path)

    node_selector = '"' + str_node_index + '"'
    yaml_selector = "        slave: " + node_selector
    with open(file_path, 'a') as f:
        f.write("      nodeSelector:\n")
        f.write(yaml_selector)
    return file_path


def getIndexFromNodeName(nodeName):
    backIndex = ""
    for i in nodeName:
        if i.isdigit():
            backIndex = backIndex + i
    return backIndex


def deploy_with_node(name, deploy_type, nodeName, update_num):
    config.load_kube_config()
    v1 = client.CoreV1Api()
    # w = watch.Watch()
    record_detailName = ""
    t_start = time.time()
    nodeIndex = getIndexFromNodeName(nodeName)
    yaml_path = yamlCreat(nodeIndex, deploy_type)
    try:
        with open(yaml_path, encoding="utf-8") as f:
            content = yaml.load(f, Loader=yaml.RoundTripLoader)
            for i in range(update_num):
                t_time = time.time()
                record_detailName = name + '-deployment' + str(deploy_type) + '-random' + str(t_time)
                content['metadata']['name'] = record_detailName
                k8s_apps_v1 = client.AppsV1Api()
                resp = k8s_apps_v1.create_namespaced_deployment(
                    body=content, namespace="default")
                logger.info("Deployment created. status='%s'" % resp.metadata.name)
    except Exception as e:
        logger.error("Error, when creating pod：{}".format(e))
        pass
    deployment_time = time.time() - t_start
    os.remove(yaml_path)
    
    has_running = False
    while not has_running:
        for x in check_pod(record_detailName, "", False):
            # print(x)
            if record_detailName in str(x.name).strip() and str(x.status).strip() == "Running":
                # logger.info("ONE POD IS RUNNNING NOW!")
                has_running = True
                return str(x.ip).strip()
        time.sleep(.05)


def deploy(yaml_path, name, deploy_type):
    try:
        t1 = time.time()
        config.load_kube_config()
        v1 = client.CoreV1Api()
    
        with open(yaml_path, encoding="utf-8") as f:
            content = yaml.load(f, Loader=yaml.RoundTripLoader)
            if deploy_type != 0:
                content['metadata']['name'] = name + '-deployment' + str(deploy_type) + '-random' +  str(time.time())
            k8s_apps_v1 = client.AppsV1Api()
            resp = k8s_apps_v1.create_namespaced_deployment(
                body=content, namespace="default")
            logger.info("Deployment created. status='%s'" % resp.metadata.name)
            logger.info("time:" + str(time.time() - t1))
    except Exception as e:
        logger.error("deploy：{}".format(e))
        pass


def findSubStr_clark(sourceStr, str, i):
    count = 0
    rs = 0
    for c in sourceStr:
        rs=rs+1
        if(c==str):
            count=count+1
        if(count==i):
            return rs


def delete_anyway(service_type, num, target_node = ""):
    try:
        deployment_list_str = os.popen("sudo kubectl get deployment | awk '{print $1}' | grep service" + str(service_type)).read()
        deployment_list_str = deployment_list_str.split()

        if num == -1:
            for i in range(len(deployment_list_str)):
                os.popen('sudo kubectl delete deployment ' + deployment_list_str[i] + ' --grace-period=0')
        else:
            service_name = NAME[int(service_type)-1]

            pod_list = check_pod(service_name, target_node)
            for each in pod_list:
                if num == 0:
                    break
                else:
                    whole_pod_name = each.name
                    # print("whole_pod_name:", whole_pod_name)
                    the_third_time_pos = findSubStr_clark(whole_pod_name, "-", 3)
                    deployment_name = whole_pod_name[0:the_third_time_pos-1]
                    delete(deployment_name)
                    num -= 1
            # for i in range(int(num)):
                # os.popen('sudo kubectl delete deployment ' + deployment_list_str[i] + ' --grace-period=0')
    except Exception as e:
        logger.error("delete_anyway：{}".format(e))


def delete(deployment_name):
    try:
        total_str = os.popen('sudo kubectl delete deployment ' + deployment_name + ' --grace-period=0').read()
    except Exception as e:
        logger.error("delete：{}".format(e))


def initDeleteAll():
    str_comd = "sudo kubectl get deployment | awk '{print $1}' | grep service | xargs kubectl delete deployment --grace-period=0"
    os.popen(str_comd).read()
    # logger.info("ALL PODS IN DEFAULT NAMESPACE HAVE BEEN DELETE.")


def initDeployAll():
    for t in range(0,1):
        for i in range(0,1):
            deploy(PATH[i], NAME[i], i+1)


def init_BE():
    BE_CONTAINER_IP_DICT = {}
    delete_anyway(0, -1)
    nodeNameList = getNodeName()
    for node_name in nodeNameList:
        ip = deploy_with_node(NAME[-1], 0, node_name, 1)
        BE_CONTAINER_IP_DICT[node_name] = ip
    return BE_CONTAINER_IP_DICT


if __name__ == "__main__":
    # initDeployAll()
    initDeleteAll()
    print(init_BE())

    # deploy_with_node(NAME[0], 1, "node83", 3)
    # delete_anyway(1,4)
    
    # initDeleteAll()
    # initDeployAll()
    
    # deploy_with_node(PATH[0], NAME[0], 1)

