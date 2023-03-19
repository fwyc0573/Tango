import json
import pickle
import time
import socket
import multiprocessing
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor
from subprocess import Popen, PIPE, STDOUT
from multiprocessing import Queue,Manager
from config.config_port import *
from config.config_service import *
from config.config_others import *
from config.config_init import *


def thread_pool_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        logger.exception("error: {}".format(worker_exception))


def run_cmd(cmd):
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = p.communicate()
    return p.returncode, stdout.strip()


def LC_BE_cpu_meo_get(pool_args): 
    each_container = pool_args
    cmd = 'docker stats %s --no-stream' %each_container
    code, out = run_cmd(cmd)
    all_list = out.decode().split()

    try:
        service_id = str(all_list[17].split("-")[1][10:])
        cpu_percent = float(all_list[18].split("%")[0])
        meo_use = float(all_list[19].split("MiB")[0])
        return service_id, cpu_percent, meo_use
    except:
        return "-1", 0, 0


def cpuLoad_send_info():
    global node_name
    pool = ThreadPoolExecutor()
    result_list = []

    while True:
        try:
            result_list.clear()
            dockerInfo_send_dict = {}
            cmd_all = 'docker ps | awk \'{print $NF}\'| grep -v "POD" | grep -v "kube" | grep -v "agent" | grep -v "NAMES" '
            code, service_name_list = run_cmd(cmd_all)
            service_name_list = service_name_list.decode().split()
            # print(service_name_list)

            for each_container in service_name_list:
                task = pool.submit(LC_BE_cpu_meo_get, (each_container))
                result_list.append(task)
                task.add_done_callback(thread_pool_callback)
            for task in result_list:
                service_id, cpu_percent, meo_use = task.result()
                dockerInfo_send_dict[service_id] = {"cpu_percent":cpu_percent, "meo_use":meo_use}
            if "-1" in dockerInfo_send_dict:
                continue
            send_list = [node_name, dockerInfo_send_dict]

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client.connect((MY_EDGE_IP, EDGE_NODE_CPU_UPDATE_PORT))

            this_mission1 = pickle.dumps(send_list)
            client.sendall(this_mission1)
            client.close()
        except Exception as e:
            logger.error("error!" + str(e))
            time.sleep(0.1)


def decision_get_make(decision_get_make_queue, ):
    node_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    node_receive_server.bind(('0.0.0.0', EDGE_NODE_LC_UPDATE_PORT))
    node_receive_server.listen(100)
    while True:
        s1, addr = node_receive_server.accept()
        data = s1.recv(1024)
        decision_get_make_queue.put(data)
        s1.close()


def THR_decision_get_make_excute(pool_args):
    data, lock_array = pool_args
    data = json.loads(data.decode('utf-8'))
    print("THR_decision_get_make_excutedata:", data)
    try:
        service_id, container_id, change_num = int(data[0]), data[1], int(data[2])
        if change_num > 0:
            changeResCg("cpu,cpuacct", container_id, "cpu.cfs_quota_us", change_num, True, service_id, lock_array)
        else:
            path_array,_ = getResPath("cpu,cpuacct", container_id, "cpu.cfs_quota_us")
            path_array.reverse()
            changeResCg("cpu,cpuacct", container_id, "cpu.cfs_quota_us", change_num, True, service_id, lock_array, path_array)
    except Exception as e:
        logger.error("Decision with error:" + str(e))


def decision_get_make_excute(decision_get_make_queue, lock_array):
    pool = ThreadPoolExecutor(max_workers=200)
    while True:
        data = decision_get_make_queue.get()
        task = pool.submit(THR_decision_get_make_excute, (data, lock_array))
        task.add_done_callback(thread_pool_callback)


def getResPath(specRes, containerID, specRes_child):
    # /sys/fs/cgroup/cpu,cpuacct/kubepods/burstable/pod6f7df9a4-4b5c-4719-b58f-c5ad743c908e
    # /sys/fs/cgroup/cpu,cpuacct/kubepods/pod7f5729f3-c054-/491f268b362c4e93459/cpu.cfs_quota_us
    
    path = "/sys/fs/cgroup/" + specRes + "/kubepods/burstable" + " | grep " + containerID + " | grep " + specRes_child
    strip_str = "/" + containerID
    path = "sudo find " + path

    try:
        cgroup_container_res = os.popen(path).read()
        cgroup_pod_res = cgroup_container_res.replace(strip_str, "")
        return [cgroup_pod_res.strip(), cgroup_container_res.strip()], specRes_child
    except Exception as e:
        logger.error("getResPath() with error:" + str(e))


def writeRes(path_array, change_num, isNumberAdd, specRes_child, serviceID, lock_array):
    # global lock_array
    now_lock = lock_array[serviceID - 1]
    with now_lock:
        for path in path_array:
            with open(path, 'w+', encoding='utf-8') as f:
                old = f.read()
                old = old.strip()
                if isNumberAdd:
                    new = int(old) + int(change_num)
                    f.write(str(new))
                else:
                    new = str(change_num)
                    f.write(new)


def changeResCg(specRes, containerID, specRes_child, change_num, isNumberAdd, serviceID, lock_array, path_array=[]):
    if path_array:
        writeRes(path_array, change_num, isNumberAdd, specRes_child, serviceID, lock_array)
    else:
        path_array, specRes_child = getResPath(specRes, containerID, specRes_child)
        writeRes(path_array, change_num, isNumberAdd, specRes_child, serviceID, lock_array)
        path_array.reverse()
        return path_array


def THR_lc_work_do(pool_args):
    """
        D-VPA function
        Since changes in memory amounts do not generally affect the latency of requests being processed, we maintain a
        memory table inside Tango to implement scaling (squeezed BE tasks will be killed in the container), and a vertical
        scaling of the container CGroup implementation for CPU type resources.
    """
    data, lock_array, record_list = pool_args

    node_get_time = time.time()
    file_chhange_time = time.time()
    # Be sure to pay attention to the order of expansion and contraction
    record_path_for_release_1 = changeResCg("cpu,cpuacct", data[1], "cpu.cfs_quota_us", int(data[2][0]), True,
                                            int(data[0][0]), lock_array)
    record_path_for_release_2 = changeResCg("cpu,cpuacct", data[1], "cpu.shares", \
                                            int(data[2][1]), True, int(data[0][0]), lock_array)
    file_chhange_time = time.time() - file_chhange_time

    use_time_start = time.time()

    pro = subprocess.Popen(data[3],shell=True, stdout=subprocess.PIPE)
    pro.wait()
    infos = pro.stdout.read()
    infos = infos.decode("utf-8")
    use_time = time.time() - use_time_start
    use_time = round(use_time * 1000, 5)
    back_result = [data[0], use_time, node_get_time]
    back_result = json.dumps(back_result)

    this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    this_client.connect((data[-1], LC_RESULT_PORT))
    this_client.sendall(bytes(back_result.encode('utf-8')))
    this_client.close()
    record_list[1] += 1

    changeResCg("cpu,cpuacct", data[1], "cpu.cfs_quota_us", int(data[2][0]) * -1, True, data[0][0], lock_array, record_path_for_release_1)
    changeResCg("cpu,cpuacct", data[1], "cpu.shares", \
                                            int(data[2][1]) * -1, True, data[0][0], lock_array, record_path_for_release_2)


def record_req_num(record_list):
    while True:
        logger.info("record：" + str(record_list))
        time.sleep(2)


def lc_work_do(lc_queue, lock_array):
    record_list = [0, 0]
    pool = ThreadPoolExecutor()

    while True:
        data = lc_queue.get()
        task = pool.submit(THR_lc_work_do, (data, lock_array, record_list))
        task.add_done_callback(thread_pool_callback)
        record_list[0] += 1


def lc_work_get(lc_queue):
    count = 0
    node_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    node_receive_server.bind(('0.0.0.0', EDGE_NODE_LC_WORK_PORT))
    node_receive_server.listen(100)

    while True:
        s1, addr = node_receive_server.accept()
        data = s1.recv(2048)
        data = json.loads(data.decode('utf-8'))
        lc_queue.put(data)
        s1.close()
        count += 1


def load_send():
    global node_name
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client.connect((MY_EDGE_IP, EDGE_NODE_LOAD_UPDATE_PORT))
            va = os.popen('uptime').read()
            index = va.find(": ")
            my_load = va[index+2:index+6]
            # print("my_load:", my_load)
            send_list = [node_name, my_load]
            # print("send_list:", send_list)
            info = json.dumps(send_list)
            client.sendall(bytes(info.encode('utf-8')))
            client.close()
        except Exception as e:
            logger.error("load_send:" + str(e))
            pass
        time.sleep(1)


def per_docker_watchdog(pool_args):
    dockerInfo_send_dict, containerID, service_id, container_watch_dict = pool_args

    if str(service_id) in LC_SERVICE_LIST:
        path = "/sys/fs/cgroup/cpu,cpuacct/kubepods/burstable" + " | grep " + containerID + " | grep " + "cpuacct.stat"
        path_2 = "/sys/fs/cgroup/memory/kubepods/burstable/" + " | grep " + containerID + " | grep " + "memory.stat"
    else:
        if VPA_open_bool:
            path = "/sys/fs/cgroup/cpu,cpuacct/kubepods/besteffort" + " | grep " + containerID + " | grep " + "cpuacct.stat"
            path_2 = "/sys/fs/cgroup/memory/kubepods/besteffort" + " | grep " + containerID + " | grep " + "memory.stat"

        else:
            path = "/sys/fs/cgroup/cpu,cpuacct/kubepods/burstable" + " | grep " + containerID + " | grep " + "cpuacct.stat"
            path_2 = "/sys/fs/cgroup/memory/kubepods/burstable/" + " | grep " + containerID + " | grep " + "memory.stat"
        
    path = "sudo find " + path
    path_2 = "sudo find " + path_2
    
    try:
        cgroup_container_cpu = os.popen(path).read()
        # print("cgroup_container_cpu:", cgroup_container_cpu)
        cgroup_container_cpu = cgroup_container_cpu.strip()
        
        cgroup_container_meo = os.popen(path_2).read()
        cgroup_container_meo = cgroup_container_meo.strip()
        # print("cgroup_container_meo:", cgroup_container_meo)
    except Exception as e:
        logger.error("getResPath() with error:" + str(e))

    while True:
        try:
            with open(cgroup_container_cpu, 'w+', encoding='utf-8') as f:
                old =  f.read()
                first_use = old.strip().split()
                utime_1 = int(first_use[1])
                stime_1 = int(first_use[3])
                # print("first_use:", first_use)
            time.sleep(0.1)

            with open(cgroup_container_cpu, 'w+', encoding='utf-8') as f:
                old = f.read()
                second_use = old.strip().split()
                utime_2 = int(second_use[1])
                stime_2 = int(second_use[3])
                # print("first_use", first_use, "second_use:", second_use)
            cpu_percent = ((utime_2 - utime_1) + (stime_2 - stime_1)) * 100.0 / (100 * 0.1 * 1 )

            # ------------------------------------------------------------------------------
            with open(cgroup_container_meo, 'w+', encoding='utf-8') as f:
                old =  f.read()
                mem_list = old.strip().split()
                # print(mem_list)
                meo_Mib = round(int(mem_list[3]) / 1024 / 1024, 2)
            # print("meo_Mib:", meo_Mib)

            dockerInfo_send_dict[service_id] = {"cpu_percent":cpu_percent, "meo_use":meo_Mib}
        except Exception as e:
            logger.error("per_docker_watchdog error:" + str(e))
            if str(service_id) in LC_SERVICE_LIST:
                path = "/sys/fs/cgroup/cpu,cpuacct/kubepods/burstable" + " | grep " + containerID + " | grep " + "cpuacct.stat"
                path_2 = "/sys/fs/cgroup/memory/kubepods/burstable/" + " | grep " + containerID + " | grep " + "memory.stat"
            else:
                path = "/sys/fs/cgroup/cpu,cpuacct/kubepods/besteffort" + " | grep " + containerID + " | grep " + "cpuacct.stat"
                path_2 = "/sys/fs/cgroup/memory/kubepods/besteffort" + " | grep " + containerID + " | grep " + "memory.stat"
                
            path = "sudo find "  + path
            path_2 = "sudo find "  + path_2
            cgroup_container_cpu = os.popen(path).read()
            cgroup_container_cpu = cgroup_container_cpu.strip()
            cgroup_container_meo = os.popen(path_2).read()
            cgroup_container_meo = cgroup_container_meo.strip()
            if cgroup_container_cpu == "":
                dockerInfo_send_dict.pop(service_id, None)
                container_watch_dict.pop(service_id, None)
                break


def container_api_search(container_check_lock,container_api_search_dict):
    while True:
        try:
            # cmd_all = 'docker ps | awk \'{print $NF}\'| grep -v "POD" | grep -v "kube" | grep -v "agent" | grep -v "NAMES" '
            cmd_all = "docker stats --no-stream --format \"{{.Name}}:{{.ID}}\" | grep -v \"POD\" | grep -v \"kube\" | grep -v \"agent\" | grep -v \"NAMES\""
            code, service_name_list = run_cmd(cmd_all)
            service_name_list = service_name_list.decode().split()
            # print("service_name_list:", service_name_list)
            # k8s_becpumeo_service0-deployment0-random1653649401.3669765-6978744d79-6jswk_default_384a3b98-435d-4d9b-a496-a631ca778a01_0:e78bfc1ea01e
            # eg. con_name + con_id
            
            with container_check_lock:
                container_api_search_dict.clear()
                for each in service_name_list:
                    service_id = each.split("-")[1][10:]
                    container_id = each.split(":")[1]
                    # print("service_id:", service_id, "container_id:", container_id)
                    container_api_search_dict[service_id] = container_id
                # print("now container_api_search_dict:", container_api_search_dict)
            time.sleep(2)
        except Exception as e:
            logger.error("container_api_search get wrong:" + str(e))
            # container_api_search_dict = service_name_list.copy()


def docker_quick_info():
    global node_name
    logger.info("docker_quick_info!")
    dockerInfo_send_dict = {}
    container_watch_dict = {}  # service_id:TASK
    container_api_search_dict = {}  # service_id:container_id
    container_check_lock = threading.Lock()
    
    threading.Thread(target=(container_api_search), args=(container_check_lock,container_api_search_dict)).start()
    pool = ThreadPoolExecutor()

    while True:
        try:
            with container_check_lock:
                for service_id in container_watch_dict:
                    if service_id not in container_api_search_dict:
                        container_watch_dict[service_id].cancelled()
                        dockerInfo_send_dict.pop(service_id, None)
                for service_id in container_api_search_dict:
                    if service_id not in container_watch_dict:
                        task = pool.submit(per_docker_watchdog, (
                        dockerInfo_send_dict, container_api_search_dict[service_id], service_id, container_watch_dict))
                        container_watch_dict[service_id] = task
                        task.add_done_callback(thread_pool_callback)
                
            send_list = [node_name, dockerInfo_send_dict]
            logger.info("DOCKER INFO：" + str(send_list))

            if dockerInfo_send_dict != {}:
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client.connect((MY_EDGE_IP, EDGE_NODE_CPU_UPDATE_PORT))
                this_mission1 = pickle.dumps(send_list)
                client.sendall(this_mission1)
                client.close()
            time.sleep(0.1)

        except Exception as e:
            logger.info("docker_quick_info failed..." + str(e))
            time.sleep(0.5)



if __name__ == '__main__':
    lock_array = []
    lc_queue = Queue(maxsize=300)
    for i in range(LC_SERVICE_NUM):
        cg_lock = multiprocessing.Lock()
        lock_array.append(cg_lock)

    # process_cpuLoad_send_info = multiprocessing.Process(target=cpuLoad_send_info) # docker info get
    # process_cpuLoad_send_info.start()

    # Custom time slot queries for containers (based on Linux CGroup)
    process_docker_quick_info = multiprocessing.Process(target=docker_quick_info)
    process_docker_quick_info.start()

    # LC request reception
    process_LC_work_get = multiprocessing.Process(target=lc_work_get, args=(lc_queue, ))
    process_LC_work_get.start()

    # Request processing and D-VPA components enable vertical scaling of resources
    for i in range(2):
        process_LC_work_do = multiprocessing.Process(target=lc_work_do, args=(lc_queue, lock_array))
        process_LC_work_do.start()

    process_LC_work_get.join()

    # process_load_send = multiprocessing.Process(target=load_send)
    # process_load_send.start()

    # decision_get_make_queue= Queue(maxsize = 500)
    # process_LCdecision_get_make = multiprocessing.Process(target=decision_get_make, args=(decision_get_make_queue, ))
    # process_LCdecision_get_make.start()
    # for i in range(2):
        # process_LCdecision_get_make = multiprocessing.Process(target=decision_get_make_excute, args=(decision_get_make_queue, lock_array))
        # process_LCdecision_get_make.start()

'''
   The resource balancer during the request processing phase can be implemented by amending the resources 
   field in the YAML file (which takes effect when the POD is initially deployed, as opposed to the resource 
   balancer's control over scheduling on the master node).
'''