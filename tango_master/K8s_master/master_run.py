from config.pack_init.config_pack import *
from checkPod import check_pod
from checkStatus import nodes, getNodeName
from deploy import deploy_with_node, delete_anyway, initDeleteAll
from config.config_port import *
from config.config_service import *
from config.config_others import *
from config.config_switch import *
from config.config_init import *
from utils import *


@atexit.register
def clean():
    initDeleteAll()


def async_run_request_be(pool_args):
    (
        req,
        MY_MEO_DICT,
        myMEO_lock_dict,
        MY_CPU_DICT,
        myCPU_lock_dict,
        tmp_writeLock,
        MY_PODLIST_DICT,
        be_run_queue,
        TEST_CPU_MEO_lock_dict,
        TEST_MY_CPU_MEO_DICT,
    ) = pool_args

    original_req_num = req[-3]
    now_try, MAX_try = 0, 20

    this_mission = json.dumps(
        {
            "masterName": master_name,
            "nodeName": req[-1],
            "service_id": req[0],
            "success": 0,
            "stuck": original_req_num,
            "failure": 0,
        }
    )
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
    client.sendall(bytes(this_mission.encode("utf-8")))
    client.close()

    try:
        t1 = time.time()
        pod_IP = run_info_get(req[0], req[-1], "BE", MY_PODLIST_DICT)
        cpu_should = BE_CPU_ONCE_NEED_DICT[str(req[0])]
        meo_need = int(BE_LOAD_DICT[str(req[0])]["mem"])

        while req[-3] > 0 and now_try < MAX_try:
            if now_try > 0:
                time.sleep(0.5)
            if DVPA_REGU_bool:
                with myMEO_lock_dict[req[-1]]["BE_MEO"], myCPU_lock_dict[req[-1]][
                    "LC_CPU_SH"
                ], myCPU_lock_dict[req[-1]]["BE_CPU_SH"]:
                    cpu_num = int(
                        (
                            MY_CPU_DICT[req[-1]]["TOTAL_INIT"]
                            - MY_CPU_DICT[req[-1]]["LC_CPU_SH"]
                            - MY_CPU_DICT[req[-1]]["BE_CPU_SH"]
                        )
                        / cpu_should
                        * 4
                    )
                    meo_num = int((MY_MEO_DICT[req[-1]]["FREE_MEO"]) / meo_need)
                    min_num = min(cpu_num, meo_num)
                    if min_num > 0:
                        min_reqNum_canNum = min(req[-3], min_num)
                        MY_CPU_DICT[req[-1]]["BE_CPU_SH"] += (
                            cpu_should * min_reqNum_canNum
                        )
                        security_meo_get(req, meo_need * min_reqNum_canNum, MY_MEO_DICT)
                        now_try += 1
                    else:
                        min_reqNum_canNum = 0
                        now_try += 1
            else:
                with TEST_CPU_MEO_lock_dict[req[-1]]["0"][
                    "CPU_SH"
                ], TEST_CPU_MEO_lock_dict[req[-1]]["0"]["MEO"]:
                    cpu_num = (
                        int(TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_CPU"] / cpu_should)
                        * 2
                    )
                    meo_num = int(
                        TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_MEO"] / meo_need
                    )
                    min_num = min(cpu_num, meo_num)
                    if min_num > 0:
                        min_reqNum_canNum = min(req[-3], min_num)
                        TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_CPU"] -= (
                            cpu_should * min_reqNum_canNum
                        )
                        TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_MEO"] -= (
                            meo_need * min_reqNum_canNum
                        )
                    else:
                        min_reqNum_canNum = 0
                        now_try += 1
            req[-3] -= min_reqNum_canNum

            this_mission = json.dumps(
                {
                    "masterName": master_name,
                    "nodeName": req[-1],
                    "service_id": req[0],
                    "success": 0,
                    "stuck": -min_reqNum_canNum,
                    "failure": 0,
                }
            )
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(
                (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
            )
            client.sendall(bytes(this_mission.encode("utf-8")))
            client.close()

            for i in range(min_reqNum_canNum):
                run_req = [pod_IP, req.copy()]
                be_run_queue.put(run_req)

        if req[-3] > 0:
            this_mission = json.dumps(
                {
                    "masterName": master_name,
                    "nodeName": req[-1],
                    "service_id": req[0],
                    "success": 0,
                    "stuck": -req[-3],
                    "failure": req[-3],
                }
            )
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(
                (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
            )
            client.sendall(bytes(this_mission.encode("utf-8")))
            client.close()

            if cloud_each_lcbe_record:
                tmp_list = [
                    req[0],
                    req[-2],
                    req[1],
                    req[3],
                    req[4],
                    req[2],
                    req[-1],
                    0,
                    -req[-3],
                ]
                tmp_list = json.dumps(tmp_list)
                this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                this_client.connect((CENTRAL_MASTER_IP, CLOUD_RESULT_PORT))
                this_client.sendall(bytes(tmp_list.encode("utf-8")))
                this_client.close()

    except Exception as e:
        this_mission = json.dumps(
            {
                "masterName": master_name,
                "nodeName": req[-1],
                "service_id": req[0],
                "success": 0,
                "stuck": -req[-3],
                "failure": 0,
            }
        )
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
        client.sendall(bytes(this_mission.encode("utf-8")))
        client.close()


def receive_be_from_other(
    MY_MEO_DICT,
    myMEO_lock_dict,
    MY_CPU_DICT,
    myCPU_lock_dict,
    MY_PODLIST_DICT,
    be_run_queue,
    TEST_CPU_MEO_lock_dict,
    TEST_MY_CPU_MEO_DICT,
):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", BE_FROM_OHTER_PORT))
    edge_master_receive_server.listen(500)
    tmp_writeLock = threading.Lock()

    pool = ThreadPoolExecutor()

    while True:
        s1, addr = edge_master_receive_server.accept()
        length_data = s1.recv(1024)
        if length_data:
            this_request = bytes() + length_data
            req = pickle.loads(this_request)
            task = pool.submit(
                async_run_request_be,
                (
                    req,
                    MY_MEO_DICT,
                    myMEO_lock_dict,
                    MY_CPU_DICT,
                    myCPU_lock_dict,
                    tmp_writeLock,
                    MY_PODLIST_DICT,
                    be_run_queue,
                    TEST_CPU_MEO_lock_dict,
                    TEST_MY_CPU_MEO_DICT,
                ),
            )
            task.add_done_callback(thread_pool_callback)
        s1.close()


def async_BE_execute_from_center(pool_args):
    (
        req,
        MY_MEO_DICT,
        MY_CPU_DICT,
        tmp_writeLock,
        MY_PODLIST_DICT,
        MY_CLUSTER_DICT,
    ) = pool_args

    if not IS_LOCAL_BE:
        this_req = pickle.dumps(req)
        client_tocloud = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_tocloud.connect((CENTRAL_MASTER_IP, DISPATCH_RECEIVE_REQUEST_PORT))
        client_tocloud.sendall(bytes(this_req))
        client_tocloud.close()
    else:
        pass


def receive_result_from_cloud(client):
    while True:
        length_data = client.recv(6)
        length = int.from_bytes(length_data, byteorder="big")
        if length == 0:
            continue
        b = bytes()
        count = 0
        while True:
            value = client.recv(length)
            b = b + value
            count += len(value)
            if count >= length:
                break
        result = pickle.loads(b)
        client.close()
        break
    return result


def BEreceive_execute_from_center(
    be_queue, MY_MEO_DICT, MY_CPU_DICT, MY_PODLIST_DICT, MY_CLUSTER_DICT
):
    pool = ThreadPoolExecutor(max_workers=5)
    tmp_writeLock = threading.Lock()

    while True:
        req = be_queue.get()
        task = pool.submit(
            async_BE_execute_from_center,
            (
                req,
                MY_MEO_DICT,
                MY_CPU_DICT,
                tmp_writeLock,
                MY_PODLIST_DICT,
                MY_CLUSTER_DICT,
            ),
        )
        task.add_done_callback(thread_pool_callback)


def async_BEreceive_receive_from_center(pool_args):
    s1, be_queue = pool_args

    length_data = s1.recv(1024)
    if length_data:
        this_request = bytes() + length_data
        req = pickle.loads(this_request)
        be_queue.put(req)


def BEreceive_receive_from_center(be_queue):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setblocking(1)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", BE_NODE_DECISION_PORT))
    edge_master_receive_server.listen(100)

    pool = ThreadPoolExecutor()

    while True:
        s1, addr = edge_master_receive_server.accept()
        task = pool.submit(async_BEreceive_receive_from_center, (s1, be_queue))
        task.add_done_callback(thread_pool_callback)


def async_receive_request_from_center(pool_args):
    s1, cache_queue, tmp_writeLock = pool_args
    length_data = s1.recv(1024 * 1)
    if length_data:
        this_request = bytes() + length_data
        req = pickle.loads(this_request)
        cache_queue.put(req)


def valueclone_nested_dict_proxy(dict_proxy):
    from multiprocessing.managers import BaseProxy

    dict_copy = dict_proxy._getvalue()
    for key, value in dict_copy.items():
        if isinstance(value, BaseProxy):
            dict_copy[key] = valueclone_nested_dict_proxy(value)
    return dict_copy


def run_info_get(service_id, target_node, task_class, MY_PODLIST_DICT):
    my_podlist_dict_tmp = valueclone_nested_dict_proxy(MY_PODLIST_DICT)
    if task_class == "BE":
        check_contaniner_id = str(0)
    else:
        check_contaniner_id = str(service_id)
    pod_list = my_podlist_dict_tmp[check_contaniner_id]
    for pod in pod_list:
        if pod.node == target_node:
            if task_class == "LC":
                return pod.ip, pod.containerID
            else:
                return pod.ip
        else:
            pass


def send_req_to_cloud_master(client, req):
    req = json.dumps(req)
    client.sendall(bytes(req.encode("utf-8")))


def receive_request_from_center(cache_queue):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setblocking(1)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", LC_NODE_DECISION_PORT))
    edge_master_receive_server.listen(100)
    tmp_writeLock = threading.Lock()

    pool = ThreadPoolExecutor(max_workers=5)

    while True:
        s1, addr = edge_master_receive_server.accept()
        task = pool.submit(
            async_receive_request_from_center, (s1, cache_queue, tmp_writeLock)
        )
        task.add_done_callback(thread_pool_callback)


def fetch_current_node_resource():
    memory_resource_of_nodes = nodes()
    return memory_resource_of_nodes


def containerCanUse(id):
    # with share_lock:
    # global CONTAINER_RECORD_LIST
    if id in CONTAINER_RECORD_LIST:
        return False
    else:
        return True


def RandomOffload_V1(
    reqIndex, pod_list, share_lock, NODE_CPU_DICT, pro_share_lock2, share_lock3
):
    waitTime = 0
    MAXTRY = 30
    with share_lock3:
        numTry = 0
        podID_list = getPodIDList(pod_list)
        randomIndex = random.randint(0, len(pod_list) - 1)

        with share_lock:
            global CONTAINER_RECORD_LIST
            bool_containerCanUse = containerCanUse(podID_list[randomIndex])
        while bool_containerCanUse == False and numTry < MAXTRY:
            time.sleep(0.1)
            randomIndex = random.randint(0, len(pod_list) - 1)
            with share_lock:
                global CONTAINER_RECORD_LIST
                bool_containerCanUse = containerCanUse(podID_list[randomIndex])
            numTry += 1

        if numTry == MAXTRY:
            return -1, -1, waitTime
        else:
            with share_lock:
                global CONTAINER_RECORD_LIST
                CONTAINER_RECORD_LIST.append(podID_list[randomIndex])
            return randomIndex, podID_list[randomIndex], waitTime


def thread_pool_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        logger.exception("erro: {}".format(worker_exception))


def container_security_meo_back(service_id, target_node, MY_MEO_DICT, pro_share_lock8):
    meo_need = CONTAINER_INIT_MEO[service_id]
    with pro_share_lock8:
        MY_MEO_DICT[target_node]["FREE_MEO"] += meo_need
        MY_MEO_DICT[target_node]["CON_MEO"] -= meo_need


def container_security_meo_get(service_id, target_node, MY_MEO_DICT, pro_share_lock8):
    meo_need = CONTAINER_INIT_MEO[service_id]
    # print("i am in container_security_meo_get,meo_need：", meo_need)
    lock_time = time.time()
    with pro_share_lock8:
        tmp_dict = valueclone_nested_dict_proxy(MY_MEO_DICT)
        if tmp_dict[target_node]["FREE_MEO"] >= meo_need:

            MY_MEO_DICT[target_node]["FREE_MEO"] -= meo_need
            MY_MEO_DICT[target_node]["CON_MEO"] += meo_need
            tmp = valueclone_nested_dict_proxy(MY_MEO_DICT)
            return True
        else:
            still_need = meo_need - tmp_dict[target_node]["FREE_MEO"]
            if tmp_dict[target_node]["BE_MEO"] >= still_need:

                current_pod_ip = BE_CONTAINER_IP_DICT[target_node]
                need_meo = "{" + '"mem":' + str(still_need) + "}"
                curl_kill_cmd = (
                    "curl "
                    + "-d '"
                    + need_meo
                    + "' "
                    + "-H 'Content-Type: application/json' "
                    + " -X POST http://"
                    + current_pod_ip
                    + ":"
                    + PORT[-1]
                    + "/killer"
                )
                pro = subprocess.Popen(
                    curl_kill_cmd, shell=True, stdout=subprocess.PIPE
                )
                pro.wait()
                infos = pro.stdout.read()
                infos = infos.decode("utf-8")
                real_kill_meo = int(infos) + still_need

                MY_MEO_DICT[target_node]["FREE_MEO"] = int(infos)
                MY_MEO_DICT[target_node]["BE_MEO"] -= real_kill_meo
                MY_MEO_DICT[target_node]["CON_MEO"] += meo_need
                lock_time = time.time() - lock_time

                return True
            else:
                return False


def async_LC_orchestra_execute(pool_args):
    # print("enter async_LC_orchestra_execute")
    (
        orchestra_req,
        node_service_all,
        WR_lock,
        MY_MEO_DICT,
        pro_share_lock8,
        delet_Lock,
    ) = pool_args
    # tmp_dict = {"service":service, "node":node, "update_num":layout_dict[service][node]}
    arr_index = int(orchestra_req["service"]) - 1
    service_id = orchestra_req["service"]
    update_num = int(orchestra_req["update_num"])
    target_node = orchestra_req["node"]
    MAX_TRY, now_try = 7, 0
    if update_num < 0:
        with delet_Lock:
            send_info = [target_node, str(service_id), update_num]
            send_info = json.dumps(send_info)
            client_tocloud = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_tocloud.connect((CENTRAL_MASTER_IP, CLOUD_LC_SET_UPDATE_PORT))
            client_tocloud.sendall(bytes(send_info.encode("utf-8")))
            client_tocloud.close()
        with delet_Lock:
            delete_anyway(orchestra_req["service"], update_num * -1, target_node)
        WR_lock.acquire_write()
        node_service_all[orchestra_req["service"]] -= update_num * -1
        WR_lock.release_write()
        container_security_meo_back(
            service_id, target_node, MY_MEO_DICT, pro_share_lock8
        )

    elif update_num > 0:
        for i in range(update_num):
            success_get_meo = False
            while success_get_meo == False and now_try < MAX_TRY:
                success_get_meo = container_security_meo_get(
                    service_id, target_node, MY_MEO_DICT, pro_share_lock8
                )
                if success_get_meo == False:
                    time.sleep(1)
                    now_try += 1
            if success_get_meo:
                deploy_with_node(NAME[arr_index], arr_index + 1, target_node, 1)
                WR_lock.acquire_write()
                node_service_all[orchestra_req["service"]] += 1

                WR_lock.release_write()
                with delet_Lock:
                    send_info = [target_node, str(service_id), 1]
                    send_info = json.dumps(send_info)
                    client_tocloud = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    client_tocloud.connect(
                        (CENTRAL_MASTER_IP, CLOUD_LC_SET_UPDATE_PORT)
                    )
                    client_tocloud.sendall(bytes(send_info.encode("utf-8")))
                    client_tocloud.close()


def LC_orchestra_execute(orchestra_queue, MY_MEO_DICT, pro_share_lock8):
    pool = ThreadPoolExecutor()
    node_service_all = {}
    for service in LC_SERVICE_LIST:
        node_service_all[service] = 0
    WR_lock = ReadWriteLock()
    delet_Lock = threading.Lock()
    tmp_list = []

    while True:
        orchestra_req = orchestra_queue.get()
        if int(orchestra_req["update_num"]) < 0:
            WR_lock.acquire_read()
            tmp_node_service_num = node_service_all[orchestra_req["service"]]
            WR_lock.release_read()
            if tmp_node_service_num <= 1:
                if orchestra_req in tmp_list:
                    time.sleep(2.5)
                    tmp_list.remove(orchestra_req)
                else:
                    orchestra_queue.put(orchestra_req)
                    tmp_list.append(orchestra_req)
                    continue

        task = pool.submit(
            async_LC_orchestra_execute,
            (
                orchestra_req,
                node_service_all,
                WR_lock,
                MY_MEO_DICT,
                pro_share_lock8,
                delet_Lock,
            ),
        )
        task.add_done_callback(thread_pool_callback)


def LC_orchestra_receive(
    orchestra_queue,
):
    edge_lcLayout_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_lcLayout_receive_server.setblocking(1)
    edge_lcLayout_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_lcLayout_receive_server.bind(("0.0.0.0", CLOUD_NODE_LC_LAYOUT_PORT))
    edge_lcLayout_receive_server.listen(100)

    while True:
        s1, addr = edge_lcLayout_receive_server.accept()
        raw_data = s1.recv(2048)
        layout_dict = json.loads(raw_data.decode("utf-8"))

        for service in layout_dict:
            for node in layout_dict[service]:
                tmp_dict = {
                    "service": service,
                    "node": node,
                    "update_num": layout_dict[service][node],
                }
                orchestra_queue.put(tmp_dict)
        s1.close()


def security_meo_back(req, MY_MEO_DICT):
    if req[-2] == "LC":
        meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]
        MY_MEO_DICT[req[-1]]["FREE_MEO"] += meo_need
        MY_MEO_DICT[req[-1]]["LC_MEO"] -= meo_need

    elif req[-2] == "BE":
        meo_need = int(BE_LOAD_DICT[str(req[0])]["mem"])
        MY_MEO_DICT[req[-1]]["FREE_MEO"] += meo_need
        MY_MEO_DICT[req[-1]]["BE_MEO"] -= meo_need


def security_meo_get(req, meo_need, MY_MEO_DICT):
    if req[-2] == "LC":
        # meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]
        if MY_MEO_DICT[req[-1]]["FREE_MEO"] >= meo_need:
            MY_MEO_DICT[req[-1]]["FREE_MEO"] -= meo_need
            MY_MEO_DICT[req[-1]]["LC_MEO"] += meo_need
        else:
            try:
                still_need = meo_need - MY_MEO_DICT[req[-1]]["FREE_MEO"]

                current_pod_ip = BE_CONTAINER_IP_DICT[req[-1]]
                need_meo = "{" + '"mem":' + str(still_need) + "}"
                curl_kill_cmd = (
                    "curl "
                    + "-d '"
                    + need_meo
                    + "' "
                    + "-H 'Content-Type: application/json' "
                    + " -X POST http://"
                    + current_pod_ip
                    + ":"
                    + PORT[-1]
                    + "/killer"
                )

                pro = subprocess.Popen(
                    curl_kill_cmd, shell=True, stdout=subprocess.PIPE
                )
                pro.wait()
                infos = pro.stdout.read()
                infos = infos.decode("utf-8")
                real_kill_meo = int(infos) + still_need

                MY_MEO_DICT[req[-1]]["FREE_MEO"] = int(infos)
                MY_MEO_DICT[req[-1]]["BE_MEO"] -= real_kill_meo
                MY_MEO_DICT[req[-1]]["LC_MEO"] += meo_need

            except Exception as e:
                wrong_info = [curl_kill_cmd]
                with open("./kill_wrong.csv", "a+", newline="") as f:
                    csv_write = csv.writer(f)
                    csv_write.writerow(wrong_info)

    elif req[-2] == "BE":
        # meo_need = int(BE_LOAD_DICT[str(req[0])]['mem'])
        MY_MEO_DICT[req[-1]]["FREE_MEO"] -= meo_need
        MY_MEO_DICT[req[-1]]["BE_MEO"] += meo_need


def security_cpu_back(req, MY_CPU_DICT):
    if req[-2] == "LC":
        cpu_should = LC_CPU_ONCE_NEED_DICT[str(req[0])][0] + req[2]
        with pro_share_lock9:
            MY_CPU_DICT[req[-1]]["LC_CPU_SH"] -= cpu_should
    elif req[-2] == "BE":
        cpu_should = BE_CPU_ONCE_NEED_DICT[str(req[0])]
        with pro_share_lock9:
            MY_CPU_DICT[req[-1]]["BE_CPU_SH"] -= cpu_should


def security_cpu_get(req, cpu_should, MY_CPU_DICT):
    if req[-2] == "LC":
        MY_CPU_DICT[req[-1]]["LC_CPU_SH"] += cpu_should
    elif req[-2] == "BE":
        MY_CPU_DICT[req[-1]]["BE_CPU_SH"] += cpu_should


def allocation_amount(num_people, amount):
    # a = [np.random.uniform(0, amount) for i in range(num_people-1)]
    a = [np.random.randint(0, amount) for i in range(num_people - 1)]
    a.append(0)
    a.append(amount)
    a.sort()
    b = [a[i + 1] - a[i] for i in range(num_people)]
    return b


def get_send_dict(req_info, real_req_num, need_sort):
    """
    ρ(·) is equipped with a random strategy which can also be changed to various priority policy as need.
    """
    random.shuffle(req_info)
    if need_sort:
        req_info.sort(key=lambda x: x[0], reverse=False)
        choose_list = req_info[:real_req_num]
    else:
        choose_list = req_info
    req_send_dict = {}  # {target1:{"num": , "delay": }
    for each in choose_list:
        if each[1] not in req_send_dict:
            req_send_dict[each[1]] = {"num": 1, "delay": each[0]}
        else:
            req_send_dict[each[1]]["num"] += 1
    return req_send_dict


def dss_policy(req, MY_CLUSTER_DICT, MY_MEO_DICT, MY_CPU_DICT):
    global start_nodes, end_nodes, unit_costs, index_dict
    # start_nodes, end_nodes, unit_costs, index_dict = init_min_max_info()
    req_num, req[4] = req[-3], "dss_policy"
    supplies = []

    if req[-2] == "LC":
        cpu_should = LC_CPU_ONCE_NEED_DICT[str(req[0])][0]
        meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]
    else:
        cpu_should = BE_CPU_ONCE_NEED_DICT[str(req[0])]
        meo_need = int(BE_LOAD_DICT[str(req[0])]["mem"])

    my_cluster_dict = valueclone_nested_dict_proxy(MY_CLUSTER_DICT)
    my_cluster_dict_noMaster = {}
    for master in my_cluster_dict:
        for node in my_cluster_dict[master]:
            my_cluster_dict_noMaster[node] = my_cluster_dict[master][node]
    for node in index_dict:
        try:
            if req[-2] == "LC":
                cpu_num = int(
                    (
                        my_cluster_dict_noMaster[node]["cpu"]["TOTAL_INIT"]
                        - my_cluster_dict_noMaster[node]["cpu"]["LC_CPU_SH"]
                    )
                    / cpu_should
                )
                meo_num = int(
                    (
                        my_cluster_dict_noMaster[node]["meo"]["FREE_MEO"]
                        + my_cluster_dict_noMaster[node]["meo"]["BE_MEO"]
                    )
                    / meo_need
                )
            else:
                cpu_num = int(
                    (
                        my_cluster_dict_noMaster[node]["cpu"]["TOTAL_INIT"]
                        - my_cluster_dict_noMaster[node]["cpu"]["LC_CPU_SH"]
                        - my_cluster_dict_noMaster[node]["cpu"]["BE_CPU_SH"]
                    )
                    / cpu_should
                )
                meo_num = int(
                    (my_cluster_dict_noMaster[node]["meo"]["FREE_MEO"]) / meo_need
                )
            min_num = min(cpu_num, meo_num)
            supplies.append(min_num)
        except:
            if node != master_name:
                supplies.append(0)

    leave_req = req_num - sum(supplies)

    # The cluster's remaining resourcesz process the batch request.
    if leave_req < 0:
        req_num_vir = sum(supplies)
        supplies = [-i for i in supplies]
        supplies.insert(index_dict[master_name], req_num_vir)
        req[2] = supplies
        MAX_INT = sum(unit_costs) * sum([abs(i) for i in supplies])
        capacities = [MAX_INT] * len(start_nodes)

        # Instantiate a SimpleMinCostFlow solver.
        min_cost_flow = pywrapgraph.SimpleMinCostFlow()
        # Add each arc.
        for i in range(0, len(start_nodes)):
            min_cost_flow.AddArcWithCapacityAndUnitCost(
                start_nodes[i], end_nodes[i], capacities[i], unit_costs[i]
            )
        # Add node supplies.
        for i in range(0, len(supplies)):
            min_cost_flow.SetNodeSupply(i, supplies[i])
        # Find the minimum cost flow between node 0 and node 4.
        if min_cost_flow.Solve() == min_cost_flow.OPTIMAL:
            # print('\n Minimum cost:', min_cost_flow.OptimalCost())
            # print('  Arc   Number  Cost')
            # for i in range(min_cost_flow.NumArcs()):
            # cost = min_cost_flow.Flow(i) * min_cost_flow.UnitCost(i)
            # print('%1s -> %1s   %3s    %3s' % (
            # min_cost_flow.Tail(i),
            # min_cost_flow.Head(i),
            # min_cost_flow.Flow(i),
            # cost))
            index = [positive for positive in supplies if positive > 0][0]
            req_info = [
                [0, supplies.index(index)] for x in range(index)
            ]  # [[delay, node]...[delay, node]]
            finished = [1 for x in range(min_cost_flow.NumArcs())]

            while sum(finished) != 0:
                for flow_index in range(min_cost_flow.NumArcs()):
                    if finished[flow_index] == 0:
                        continue

                    if min_cost_flow.Flow(flow_index) == 0:
                        finished[flow_index] = 0
                        continue

                    start_num = sum(
                        [1 for x in req_info if x[1] == min_cost_flow.Tail(flow_index)]
                    )
                    if start_num < min_cost_flow.Flow(flow_index):
                        continue

                    finished[flow_index] = 0
                    req_num_tmp = 0
                    for req_index in range(len(req_info)):
                        if req_num_tmp == min_cost_flow.Flow(flow_index):
                            break
                        if req_info[req_index][1] == min_cost_flow.Tail(flow_index):
                            req_info[req_index][1] = min_cost_flow.Head(flow_index)
                            req_info[req_index][0] = (
                                req_info[req_index][0] + unit_costs[flow_index]
                            )
                            req_num_tmp = req_num_tmp + 1

            req_send_dict = get_send_dict(req_info, req_num, True)
            for index_id in req_send_dict:
                for node in index_dict:
                    if index_dict[node] == index_id:
                        node_name = node
                        div_req = req.copy()
                        div_req[-1] = node_name
                        div_req[-3] = req_send_dict[index_id]["num"]
                        div_req[-4] = req_send_dict[index_id]["delay"]

                        master_ip = get_master_ip(div_req[-1])
                        if div_req[-2] == "LC":
                            send_req_to_port(div_req, master_ip, LC_FROM_OHTER_PORT)
                        else:
                            send_req_to_port(div_req, master_ip, BE_FROM_OHTER_PORT)
                        break
        else:
            print("There was an issue with the min cost flow input.")
            logger.info(str(min_cost_flow.Solve()))
            logger.info(str(min_cost_flow.OPTIMAL))
            raise
    # The cluster's remaining resources cannot fully process the batch request.
    else:
        # Assign some of the requests to the node with the remaining resources first
        req_num = sum(supplies)
        req_num_vir = sum(supplies)
        supplies = [-i for i in supplies]
        supplies.insert(index_dict[master_name], req_num_vir)
        req[2] = supplies
        MAX_INT = sum(unit_costs) * sum([abs(i) for i in supplies])
        capacities = [MAX_INT] * len(start_nodes)

        # Instantiate a SimpleMinCostFlow solver.
        min_cost_flow = pywrapgraph.SimpleMinCostFlow()
        # Add each arc.
        for i in range(0, len(start_nodes)):
            min_cost_flow.AddArcWithCapacityAndUnitCost(
                start_nodes[i], end_nodes[i], capacities[i], unit_costs[i]
            )
        # Add node supplies.
        for i in range(0, len(supplies)):
            min_cost_flow.SetNodeSupply(i, supplies[i])

        if min_cost_flow.Solve() == min_cost_flow.OPTIMAL:
            index = [positive for positive in supplies if positive > 0][0]
            req_info = [
                [0, supplies.index(index)] for x in range(index)
            ]  # [[delay, node]...[delay, node]]
            # print(index, supplies.index(index))
            finished = [1 for x in range(min_cost_flow.NumArcs())]

            while sum(finished) != 0:
                for flow_index in range(min_cost_flow.NumArcs()):
                    if finished[flow_index] == 0:
                        continue
                    if min_cost_flow.Flow(flow_index) == 0:
                        finished[flow_index] = 0
                        continue
                    start_num = sum(
                        [1 for x in req_info if x[1] == min_cost_flow.Tail(flow_index)]
                    )
                    if start_num < min_cost_flow.Flow(flow_index):
                        continue
                    finished[flow_index] = 0
                    req_num_tmp = 0
                    for req_index in range(len(req_info)):
                        if req_num_tmp == min_cost_flow.Flow(flow_index):
                            break
                        if req_info[req_index][1] == min_cost_flow.Tail(flow_index):
                            req_info[req_index][1] = min_cost_flow.Head(flow_index)
                            req_info[req_index][0] = (
                                req_info[req_index][0] + unit_costs[flow_index]
                            )
                            req_num_tmp = req_num_tmp + 1
            req_send_dict = get_send_dict(req_info, req_num, True)
            for index_id in req_send_dict:
                for node in index_dict:
                    if index_dict[node] == index_id:
                        node_name = node
                        div_req = req.copy()
                        div_req[-1] = node_name
                        div_req[-3] = req_send_dict[index_id]["num"]
                        div_req[-4] = req_send_dict[index_id]["delay"]

                        master_ip = get_master_ip(div_req[-1])
                        if div_req[-2] == "LC":
                            send_req_to_port(div_req, master_ip, LC_FROM_OHTER_PORT)
                        else:
                            send_req_to_port(div_req, master_ip, BE_FROM_OHTER_PORT)
                        break
        else:
            logger.info("There was an issue with the min cost flow input.")
            raise

        # Then the other requests are distributed to the nodes according to their capabilities (since the nodes are
        # homogeneous in a real cluster, they are distributed equally here)
        node_list = []
        for master in EDGEMASTER_IP_DICTSET:
            node_list += list(EDGEMASTER_IP_DICTSET[master]["nodeset"])
        for i in range(len(node_list)):
            if i < len(node_list) - 1:
                req_div_num = int(leave_req * -1 / len(node_list))
            else:
                req_div_num = int(leave_req * -1 / len(node_list)) + (
                    leave_req * -1
                ) % len(node_list)
            div_req = req.copy()
            div_req[-1] = node_list[i]
            div_req[-3] = req_div_num
            master_ip = get_master_ip(div_req[-1])

            if div_req[-2] == "LC":
                send_req_to_port(div_req, master_ip, LC_FROM_OHTER_PORT)
            else:
                send_req_to_port(div_req, master_ip, BE_FROM_OHTER_PORT)


def send_req_to_port(req, target_ip, port):
    req = pickle.dumps(req)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((target_ip, port))
    client.sendall(bytes(req))
    client.close()


# LC dispatcher
def distributed_scheduling_dispatcher(pool_args):
    req, tmp_writeLock, MY_MEO_DICT, MY_CPU_DICT, MY_CLUSTER_DICT = pool_args

    dss_policy(req, MY_CLUSTER_DICT, MY_MEO_DICT, MY_CPU_DICT)


def execute_request_from_center(cache_queue, MY_MEO_DICT, MY_CPU_DICT, MY_CLUSTER_DICT):
    tmp_writeLock = threading.Lock()

    pool = ThreadPoolExecutor()
    while True:
        req = cache_queue.get()
        task = pool.submit(
            distributed_scheduling_dispatcher,
            (req, tmp_writeLock, MY_MEO_DICT, MY_CPU_DICT, MY_CLUSTER_DICT),
        )
        task.add_done_callback(thread_pool_callback)


def receive_lc_from_other(
    lc_excute_queue,
):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", LC_FROM_OHTER_PORT))
    edge_master_receive_server.listen(100)
    tmp_writeLock = threading.Lock()

    pool = ThreadPoolExecutor(max_workers=10)

    while True:
        s1, addr = edge_master_receive_server.accept()
        task = pool.submit(
            async_receive_request_from_other, (s1, lc_excute_queue, tmp_writeLock)
        )
        task.add_done_callback(thread_pool_callback)


def async_receive_request_from_other(pool_args):
    s1, lc_excute_queue, tmp_writeLock = pool_args
    length_data = s1.recv(1024)
    if length_data:
        this_request = bytes() + length_data
        req = pickle.loads(this_request)
        lc_excute_queue.put(req)


def run_request_lc(
    lc_excute_queue,
    MY_MEO_DICT,
    myMEO_lock_dict,
    MY_CPU_DICT,
    myCPU_lock_dict,
    MY_PODLIST_DICT,
    lc_run_queue,
    MY_QOS_SERVICE_DICT,
    TEST_CPU_MEO_lock_dict,
    TEST_MY_CPU_MEO_DICT,
):

    tmp_writeLock = threading.Lock()
    pool = ThreadPoolExecutor()
    while True:
        req = lc_excute_queue.get()
        if req[-3] > 0:
            task = pool.submit(
                thr_run_request_lc,
                (
                    req,
                    tmp_writeLock,
                    MY_MEO_DICT,
                    myMEO_lock_dict,
                    MY_CPU_DICT,
                    myCPU_lock_dict,
                    MY_PODLIST_DICT,
                    lc_run_queue,
                    MY_QOS_SERVICE_DICT,
                    TEST_CPU_MEO_lock_dict,
                    TEST_MY_CPU_MEO_DICT,
                ),
            )
            task.add_done_callback(thread_pool_callback)


def run_run_run(pool_args):
    (
        pod_IP,
        req,
        conta_id,
        MY_CPU_DICT,
        myCPU_lock_dict,
        myMEO_lock_dict,
        MY_MEO_DICT,
        tmp_write_lock,
        TEST_CPU_MEO_lock_dict,
        TEST_MY_CPU_MEO_DICT,
    ) = pool_args
    if req[-2] == "LC":
        get_error = local_run(pod_IP, req, conta_id)
        if get_error:
            if DVPA_REGU_bool:
                with myMEO_lock_dict[req[-1]]["LC_MEO"]:
                    security_meo_back(req, MY_MEO_DICT)
                with myCPU_lock_dict[req[-1]]["LC_CPU_SH"]:
                    security_cpu_back(req, MY_CPU_DICT)
            else:
                cpu_should = LC_CPU_ONCE_NEED_DICT[str(req[0])][0] + req[2]
                meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]
                with TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])][
                    "CPU_SH"
                ], TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])]["MEO"]:
                    TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_CPU"] += cpu_should
                    TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_MEO"] += meo_need

            if cloud_each_task_situation:
                this_mission2 = json.dumps(
                    {
                        "masterName": master_name,
                        "nodeName": req[-1],
                        "service_id": req[0],
                        "success": 0,
                        "stuck": 0,
                        "failure": 1,
                    }
                )
                client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client2.connect(
                    (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
                )
                client2.sendall(bytes(this_mission2.encode("utf-8")))
                client2.close()
    else:
        useTime, was_killed = local_run(pod_IP, req)
        if DVPA_REGU_bool:
            if not was_killed:
                with myMEO_lock_dict[req[-1]]["BE_MEO"]:
                    security_meo_back(req, MY_MEO_DICT)
            with myCPU_lock_dict[req[-1]]["BE_CPU_SH"]:
                security_cpu_back(req, MY_CPU_DICT)
        else:
            cpu_should = BE_CPU_ONCE_NEED_DICT[str(req[0])]
            meo_need = int(BE_LOAD_DICT[str(req[0])]["mem"])
            with TEST_CPU_MEO_lock_dict[req[-1]]["0"]["CPU_SH"], TEST_CPU_MEO_lock_dict[
                req[-1]
            ]["0"]["MEO"]:
                TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_CPU"] += cpu_should
                TEST_MY_CPU_MEO_DICT[req[-1]]["0"]["AVAI_MEO"] += meo_need

        if useTime == 0:
            runtime = 99999999
        else:
            distance = (
                float(
                    distEclud(
                        np.array(node_position[master_name]),
                        np.array(node_position[req[3]]),
                    )
                )
                + node_trans_property[master_name]
                + node_trans_property[req[3]]
            )
            runtime = time.time() - req[1]

        if runtime < 99999999:
            detail_mission = {
                "masterName": master_name,
                "nodeName": req[-1],
                "service_id": req[0],
                "success": 1,
                "stuck": 0,
                "failure": 0,
            }
        else:
            detail_mission = {
                "masterName": master_name,
                "nodeName": req[-1],
                "service_id": req[0],
                "success": 0,
                "stuck": 0,
                "failure": 1,
            }

        if cloud_each_task_situation:
            this_mission2 = json.dumps(detail_mission)
            client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client2.connect(
                (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
            )
            client2.sendall(bytes(this_mission2.encode("utf-8")))
            client2.close()

        if runtime != 99999999 and cloud_each_lcbe_record:
            tmp_list = [
                req[0],
                req[-2],
                req[1],
                req[3],
                req[4],
                req[2],
                req[5],
                req[-1],
                distance,
                runtime,
            ]

            # with tmp_write_lock:
            # with open('./collect_req.csv', 'a+', newline="") as f:
            # csv_write = csv.writer(f)
            # csv_write.writerow(tmp_list)

            tmp_list = json.dumps(tmp_list)
            this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            this_client.connect((CENTRAL_MASTER_IP, CLOUD_RESULT_PORT))
            this_client.sendall(bytes(tmp_list.encode("utf-8")))
            this_client.close()


def thr_run_request_lc(pool_args):
    (
        req,
        tmp_writeLock,
        MY_MEO_DICT,
        myMEO_lock_dict,
        MY_CPU_DICT,
        myCPU_lock_dict,
        MY_PODLIST_DICT,
        lc_run_queue,
        MY_QOS_SERVICE_DICT,
        TEST_CPU_MEO_lock_dict,
        TEST_MY_CPU_MEO_DICT,
    ) = pool_args

    MAX_try, now_try = (
        5 + int(ALL_SERVICE_EXECUTE_STANDARDTIME[str(req[0])] / 50) * 1,
        0,
    )
    # MAX_try = 1
    this_mission = json.dumps(
        {
            "masterName": master_name,
            "nodeName": req[-1],
            "service_id": req[0],
            "success": 0,
            "stuck": req[-3],
            "failure": 0,
        }
    )
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
    client.sendall(bytes(this_mission.encode("utf-8")))
    client.close()
    try:
        pod_IP, conta_id = run_info_get(req[0], req[-1], "LC", MY_PODLIST_DICT)
        req[2] = MY_QOS_SERVICE_DICT[req[-1]][str(req[0])]
        cpu_should = LC_CPU_ONCE_NEED_DICT[str(req[0])][0] + req[2]
        meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]

        while req[-3] > 0 and now_try < MAX_try:
            if now_try > 0:
                time.sleep(random.uniform(0.01, 0.1))
            if DVPA_REGU_bool:
                with myMEO_lock_dict[req[-1]]["BE_MEO"], myMEO_lock_dict[req[-1]][
                    "LC_MEO"
                ], myCPU_lock_dict[req[-1]]["LC_CPU_SH"]:
                    cpu_num = int(
                        (
                            MY_CPU_DICT[req[-1]]["TOTAL_INIT"]
                            - MY_CPU_DICT[req[-1]]["LC_CPU_SH"]
                        )
                        / cpu_should
                    )
                    meo_num = int(
                        (
                            MY_MEO_DICT[req[-1]]["FREE_MEO"]
                            + MY_MEO_DICT[req[-1]]["BE_MEO"]
                        )
                        / meo_need
                    )
                    min_num = min(cpu_num, meo_num)
                    if min_num > 0:
                        min_reqNum_canNum = min(req[-3], min_num)
                        security_meo_get(req, meo_need * min_reqNum_canNum, MY_MEO_DICT)
                        MY_CPU_DICT[req[-1]]["LC_CPU_SH"] += (
                            cpu_should * min_reqNum_canNum
                        )
                    else:
                        min_reqNum_canNum = 0
                        now_try += 1
            else:
                with TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])][
                    "CPU_SH"
                ], TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])]["MEO"]:
                    cpu_num = int(
                        TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_CPU"]
                        / cpu_should
                    )
                    meo_num = int(
                        TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_MEO"]
                        / meo_need
                    )
                    min_num = min(cpu_num, meo_num)
                    if min_num > 0:
                        min_reqNum_canNum = min(req[-3], min_num)
                        TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_CPU"] -= (
                            cpu_should * min_reqNum_canNum
                        )
                        TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_MEO"] -= (
                            meo_need * min_reqNum_canNum
                        )
                    else:
                        min_reqNum_canNum = 0
                        now_try += 1
            req[-3] -= min_reqNum_canNum

            this_mission = json.dumps(
                {
                    "masterName": master_name,
                    "nodeName": req[-1],
                    "service_id": req[0],
                    "success": 0,
                    "stuck": -min_reqNum_canNum,
                    "failure": 0,
                }
            )
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(
                (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
            )
            client.sendall(bytes(this_mission.encode("utf-8")))
            client.close()

            for i in range(min_reqNum_canNum):
                run_req = [pod_IP, req.copy(), conta_id]
                lc_run_queue.put(run_req)

        if req[-3] > 0:
            this_mission = json.dumps(
                {
                    "masterName": master_name,
                    "nodeName": req[-1],
                    "service_id": req[0],
                    "success": 0,
                    "stuck": -req[-3],
                    "failure": 0,
                }
            )
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(
                (CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT)
            )
            client.sendall(bytes(this_mission.encode("utf-8")))
            client.close()

            if cloud_each_lcbe_record:
                # Log information about discarded requests
                tmp_list = [
                    req[0],
                    req[-2],
                    req[1],
                    req[3],
                    req[4],
                    req[2],
                    req[-1],
                    0,
                    -req[-3],
                ]
                tmp_list = json.dumps(tmp_list)
                this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                this_client.connect((CENTRAL_MASTER_IP, CLOUD_RESULT_PORT))
                this_client.sendall(bytes(tmp_list.encode("utf-8")))
                this_client.close()
            # this_mission = json.dumps({req[-2]:{"success": 0, "failure": req[-3], "stuck": -req[-3]}})
            # this_mission = json.dumps({req[-2]:{"success": 0, "failure": 0, "stuck": -req[-3]}})
            # this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # this_client.connect((CENTRAL_MASTER_IP, CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT))
            # this_client.sendall(bytes(this_mission.encode('utf-8')))
            # this_client.close()

    except Exception as e:
        raise
        # There is no service of this type, which will be handled as failure
        this_mission = json.dumps(
            {
                "masterName": master_name,
                "nodeName": req[-1],
                "service_id": req[0],
                "success": 0,
                "stuck": -req[-3],
                "failure": 0,
            }
        )
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
        client.sendall(bytes(this_mission.encode("utf-8")))
        client.close()
        # this_mission = json.dumps({req[-2]:{"success": 0, "failure": req[-3], "stuck": -req[-3]}})
        # this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # this_client.connect((CENTRAL_MASTER_IP, CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT))
        # this_client.sendall(bytes(this_mission.encode('utf-8')))
        # this_client.close()


def THR_myCluster_dockerStats_send(dockerStats_lock, myCluster_dockerStats_dict):
    # docker_stats_info_dict = {}
    while True:
        docker_stats_info_dict = {master_name: {}}
        with dockerStats_lock:
            # docker_stats_info_dict = myCluster_dockerStats_dict.copy()
            # print("docker_stats_info_dict:", docker_stats_info_dict)
            for each_node in myCluster_dockerStats_dict:
                docker_stats_info_dict[master_name][each_node] = {}
                for each_service_id in myCluster_dockerStats_dict[each_node]:
                    docker_stats_info_dict[master_name][each_node][each_service_id] = {}
                    docker_stats_info_dict[master_name][each_node][each_service_id][
                        "meo_use"
                    ] = (
                        CONTAINER_INIT_MEO[each_service_id]
                        + myCluster_dockerStats_dict[each_node][each_service_id][
                            "meo_use"
                        ]
                    )
                    docker_stats_info_dict[master_name][each_node][each_service_id][
                        "meo_percent"
                    ] = round(
                        myCluster_dockerStats_dict[each_node][each_service_id][
                            "meo_use"
                        ]
                        / MEO_TOTAL_INIT,
                        3,
                    )
                    docker_stats_info_dict[master_name][each_node][each_service_id][
                        "cpu_percent"
                    ] = myCluster_dockerStats_dict[each_node][each_service_id][
                        "cpu_percent"
                    ]
                    docker_stats_info_dict[master_name][each_node][each_service_id][
                        "cpu_use"
                    ] = (
                        myCluster_dockerStats_dict[each_node][each_service_id][
                            "cpu_percent"
                        ]
                        / 100
                        * 100000
                    )

        docker_stats_info = json.dumps(docker_stats_info_dict)
        client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        client2.connect((CENTRAL_MASTER_IP, RESOURCE_ON_EACH_NODE_PORT))
        client2.send(len(docker_stats_info).to_bytes(length=6, byteorder="big"))
        client2.sendall(bytes(docker_stats_info.encode("utf-8")))
        client2.close()
        time.sleep(0.05)


def node_container_get_send(MY_CPU_DICT):
    """
    state storage
    In order to get more real-time information (especially the resource changes brought by LC requests), we write
    custom query resource usage API on worker to get resource information.
    """

    count = 0
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", EDGE_NODE_CPU_UPDATE_PORT))
    server.listen(200)

    dockerStats_lock = threading.Lock()
    myCluster_dockerStats_dict = {}
    for node in EDGEMASTER_IP_DICTSET[master_name]["nodeset"]:
        myCluster_dockerStats_dict[node] = {}

    thr = threading.Thread(
        target=THR_myCluster_dockerStats_send,
        args=(dockerStats_lock, myCluster_dockerStats_dict),
    )
    thr.start()

    while True:
        if count != 0 and count % 1000 == 0:
            gc.collect()
        client, addr = server.accept()
        try:
            this_request = bytes() + client.recv(1024 * 2)
            result = pickle.loads(this_request)
            # {'1': {'cpu_percent': 0.02, 'meo_use': 24.3}, '0': {'cpu_percent': 0.01, 'meo_use': 25.15}}
            node_name = result[0]
            res_dict = result[1]
            with dockerStats_lock:
                myCluster_dockerStats_dict[node_name] = res_dict
        except Exception as e:
            print("node_container_get_send recv failed:", str(e))
            break
        client.close()
        count += 1


def local_run(current_pod_ip, req, conta_id=""):
    start_time = time.time()
    if req[-2] == "LC":
        arr_index = int(req[0]) - 1
        try:
            tmp_dict = (
                '{"cpu":'
                + LC_LOAD_DICT[str(req[0])]["cpu"]
                + ',"mem":'
                + LC_LOAD_DICT[str(req[0])]["mem"]
                + "}"
            )
            curl_cmd = (
                "curl "
                + "-d '"
                + tmp_dict
                + "' "
                + "-H 'Content-Type: application/json' "
                + " -X POST http://"
                + current_pod_ip
                + ":"
                + PORT[arr_index]
                + "/lcservice"
            )
            client_node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ip_target = name_to_ip(req[-1])
            """
                master schedules requests to worker while adjusting the resource usage limit of service instances 
                deployed by worker in real time to achieve elastic scheduling.
            """
            if DVPA_REGU_bool:
                lc_cpu_list = [
                    LC_CPU_ONCE_NEED_DICT[str(req[0])][0] + req[2],
                    LC_CPU_ONCE_NEED_DICT[str(req[0])][1],
                ]
            else:
                lc_cpu_list = [0, 0]

            sendinfo__array = [
                req,
                conta_id,
                lc_cpu_list,
                curl_cmd,
                EDGEMASTER_IP_DICTSET[master_name]["IP"],
            ]
            client_node.connect((ip_target, EDGE_NODE_LC_WORK_PORT))
            send_curl_cg = json.dumps(sendinfo__array)
            client_node.sendall(bytes(send_curl_cg.encode("utf-8")))
            client_node.close()
        except Exception as e:
            logger.error("error：{}".format(e))
            return True
        return False
    elif req[-2] == "BE":
        arr_index = -1
        try:
            tmp_dict = (
                '{"cpu":'
                + BE_LOAD_DICT[str(req[0])]["cpu"]
                + ',"mem":'
                + BE_LOAD_DICT[str(req[0])]["mem"]
                + "}"
            )
            curl_cmd = (
                "curl "
                + "-d '"
                + tmp_dict
                + "' "
                + "-H 'Content-Type: application/json' "
                + " -X POST http://"
                + current_pod_ip
                + ":"
                + PORT[arr_index]
                + "/beservice"
            )
            pro = subprocess.Popen(curl_cmd, shell=True, stdout=subprocess.PIPE)
            pro.wait()
            useTime = round((time.time() - start_time) * 1000, 2)
            infos = pro.stdout.read()
            infos = infos.decode("utf-8")

            # If the BE request is killed by LC during processing
            if "TASK KILLED" in infos:
                return 0, True
        except Exception as e:
            logger.error("error：{}".format(e))
            return 0, False
        return useTime, False


def TEST_EACH_SERVICE_TIME():
    t = 0
    for i in range(100):
        t1 = time.time()
        ip = ""
        os.popen(
            "curl -X POST -F image=@./datasets/"
            + NAME[1]
            + "/test.jpg"
            + " 'http://"
            + ip
            + ":"
            + PORT[1]
            + "/predict'"
        ).read()
        t2 = time.time()
        t += t2 - t1
    avg_t = t / 100
    print(avg_t)


def podInfo_Send(pro_share_lock1):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setblocking(1)
    client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client.connect((CENTRAL_MASTER_IP, CURRENT_SERVICE_ON_EACH_NODE_PORT))
    fail_num_count = 0

    while True:
        update_info = []
        with pro_share_lock1:
            for i in range(len(NAME)):
                tmp = check_pod(NAME[i])
                if len(tmp) != 0:
                    update_info.extend(tmp)
        try:
            send_req_to_cloud_master(
                client, [master_name, [pod.__dict__ for pod in update_info]]
            )
        except Exception as e:
            if fail_num_count < 3:
                fail_num_count += 1
            else:
                client.close()
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setblocking(1)
                client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client.connect((CENTRAL_MASTER_IP, CURRENT_SERVICE_ON_EACH_NODE_PORT))
                fail_num_count = 0
                print("podInfo_Send:", e, "; CONNECT HAS BEEN RESET NOW ")
        time.sleep(0.5)


def cluster_Send(
    NODE_meo_DICT,
    pro_share_lock7,
    NODE_CPU_DICT,
    pro_share_lock2,
    NODE_load_DICT,
    pro_share_lock3,
    pro_share_lock6,
    NODE_LC_DICT,
):
    index = 0
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_bandwidth = np.random.randint(600, 1801)
    # changBandWidth(master_bandwidth)
    client.connect((CENTRAL_MASTER_IP, COLLECT_CLUSTER_INFO))
    fail_num_count = 0
    record_data_master_delay = 0
    while True:
        ip = name_to_ip(master_name)
        index += 1
        if index % 5 == 0:
            master_bandwidth = np.random.randint(600, 3073)
            changBandWidth(master_bandwidth)

        try:
            master_delay = ping(CENTRAL_MASTER_IP, 3, 1000)
            record_data_master_delay = master_delay
        except Exception as e:
            master_delay = record_data_master_delay

        with pro_share_lock2:
            CPU_DICT = valueclone_nested_dict_proxy(NODE_CPU_DICT)

        with pro_share_lock3:
            load_DICT = valueclone_nested_dict_proxy(NODE_load_DICT)

        with pro_share_lock6:
            LC_DICT = valueclone_nested_dict_proxy(NODE_LC_DICT)

        with pro_share_lock7:
            meo_DICT = valueclone_nested_dict_proxy(NODE_meo_DICT)

        resource = {master_name: {}}
        current_resource = {}
        current_resource["ip"] = ip
        current_resource["master_bandwidth"] = master_bandwidth
        current_resource["master_delay"] = master_delay
        current_resource["nodes_cpu"] = CPU_DICT
        current_resource["nodes_load"] = load_DICT
        current_resource["lc_taillatency"] = LC_DICT
        current_resource["nodes_meo"] = meo_DICT
        resource[master_name] = current_resource
        cluster = json.dumps(resource)
        try:
            client.sendall(bytes(cluster.encode("utf-8")))
        except Exception as e:
            if fail_num_count < 3:
                fail_num_count += 1
            else:
                client.close()
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect((CENTRAL_MASTER_IP, COLLECT_CLUSTER_INFO))
                fail_num_count = 0
        time.sleep(0.05)


global_raw_dict_lc = {}
global_can_desicion_make = False


def clear_all(*dict_proxies):
    for dict_proxy in dict_proxies:
        for key, value in dict_proxy.items():
            dict_proxy[key].clear()


def THR_collect_LC_info(server, queue):
    while True:
        conn, addr = server.accept()
        lc_service_bytes = conn.recv(2048)
        lc_service_bytes = json.loads(lc_service_bytes.decode("utf-8"))
        # print("new lc_service info:", lc_service_bytes)
        queue.put(lc_service_bytes)
        conn.close()


def collect_LC_info(queue, server):
    for i in range(3):
        thr = threading.Thread(target=THR_collect_LC_info, args=(server, queue))
        thr.start()


def THR_tidy_LC_info(queue, share_lock, share_lock2, NODE_LC_DICT, pro_share_lock6):
    time.sleep(1)

    num_count = 0
    time_step = 0.1
    timeWindow = 0
    is_first = True
    raw_dict_lc = {}
    desicion_dict_lc = {}

    while True:
        if not queue.empty():
            lc_dict = queue.get()

            if is_first:
                timeWindow = time.time() + time_step
                is_first = False
            if lc_dict["time_get"] < timeWindow:
                num_count += 1
                if lc_dict["node"] not in raw_dict_lc:
                    raw_dict_lc[lc_dict["node"]] = {}
                    desicion_dict_lc[lc_dict["node"]] = {}
                if lc_dict["LCtype"] not in raw_dict_lc[lc_dict["node"]]:
                    raw_dict_lc[lc_dict["node"]][lc_dict["LCtype"]] = {
                        "BreNum": 0,
                        "ObeyNum": 0,
                        "slackScore": [],
                    }
                    desicion_dict_lc[lc_dict["node"]][lc_dict["LCtype"]] = {
                        "BreNum": 0,
                        "ObeyNum": 0,
                        "slackScore": 0,
                    }
                raw_dict_lc[lc_dict["node"]][lc_dict["LCtype"]]["slackScore"].append(
                    lc_dict["slackScore"]
                )
            else:
                for node in raw_dict_lc:
                    with pro_share_lock6:
                        tmp1_dict = NODE_LC_DICT.setdefault(node, manager.dict())
                    for service_type in raw_dict_lc[node]:
                        BreNumAdd = ObeyNmumAdd = 0
                        SLO_SLACK_DEGREE_arr = raw_dict_lc[node][service_type][
                            "slackScore"
                        ]
                        result_SLO_SLACK_DEGREE = round(
                            np.mean(SLO_SLACK_DEGREE_arr), 5
                        )
                        desicion_dict_lc[node][service_type][
                            "slackScore"
                        ] = result_SLO_SLACK_DEGREE
                        if result_SLO_SLACK_DEGREE >= 0:
                            BreNumAdd = 1
                            desicion_dict_lc[node][service_type]["BreNum"] = 1
                        else:
                            ObeyNmumAdd = 1
                            desicion_dict_lc[node][service_type]["ObeyNum"] = 1

                        with pro_share_lock6:
                            if service_type in tmp1_dict:
                                tmp2_dict = tmp1_dict[service_type]
                            else:
                                tmp2_dict = manager.dict(
                                    {"BreNum": 0, "ObeyNum": 0, "slackScore": 0}
                                )
                                tmp1_dict[service_type] = tmp2_dict
                            tmp2_dict["BreNum"] += BreNumAdd
                            tmp2_dict["ObeyNum"] += ObeyNmumAdd
                            tmp2_dict["slackScore"] += result_SLO_SLACK_DEGREE

                with share_lock, share_lock2:
                    global global_raw_dict_lc
                    global global_can_desicion_make
                    global_raw_dict_lc = desicion_dict_lc.copy()

                global_can_desicion_make = True

                raw_dict_lc.clear()
                desicion_dict_lc.clear()
                num_count = 0
                timeWindow = lc_dict["time_get"] + time_step
                queue.put(lc_dict)
        else:
            pass


def THR_decide_LC_movement(share_lock, share_lock2):
    tmp_record_canDesicion = False
    tmp_record_raw_dict = {}
    node_IP_List = []
    nodeNameList = getNodeName()
    for nodename in nodeNameList:
        node_IP_List.append(name_to_ip(nodename))
    while True:
        with share_lock2:
            global global_can_desicion_make
            tmp_record_canDesicion = global_can_desicion_make
        if tmp_record_canDesicion:
            with share_lock:
                global global_raw_dict_lc
            with share_lock2:
                global_can_desicion_make = False


def core_LCRecivMind(NODE_LC_DICT, pro_share_lock6):
    queue = Thread_Queue(40)
    share_lock = threading.Lock()
    share_lock2 = threading.Lock()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", COLLECT_LC_INFO))
    server.listen(100)
    collect_LC_info(queue, server)
    # eye = threading.Thread(target=collect_LC_info, args=(queue,))
    tidy = threading.Thread(
        target=THR_tidy_LC_info,
        args=(queue, share_lock, share_lock2, NODE_LC_DICT, pro_share_lock6),
    )
    mind = threading.Thread(
        target=THR_decide_LC_movement, args=(share_lock, share_lock2)
    )
    # eye.start()
    tidy.start()
    mind.start()
    tidy.join()


def THR_check_LC_policy(node_qos_dict, thr_lock, start_counter, MY_QOS_SERVICE_DICT):
    """
    LC QoS re-assurer function
    Since increasing or decreasing memory does not directly affect the processing delay of the processed request
    (our service program does not involve disk read / write services), Tango's adjustment of resources only
    involves the CPU.However, you can expand the resource requirements as needed. You only need to attach the
    information variable to the worker and then pass it to the worker for control.
    """

    time_windows = 0.1
    per_cpu_add = 20
    per_cpu_sub = 10
    threshold_a = 0.1
    threshold_b = 0.3

    while True:
        if start_counter[0] > 5:
            for node in node_qos_dict:
                for service_id in node_qos_dict[node]:
                    max_range = LC_CPU_ONCE_NEED_DICT[service_id][0] / 3
                    min_range = LC_CPU_ONCE_NEED_DICT[service_id][0] / 5 * -1
                    try:
                        if len(node_qos_dict[node][service_id]["time_list"]) > 0:
                            avg_usetime = round(
                                np.mean(node_qos_dict[node][service_id]["time_list"]), 4
                            )
                            slackScore = round(
                                (
                                    ALL_SERVICE_EXECUTE_STANDARDTIME[service_id]
                                    - avg_usetime
                                )
                                / ALL_SERVICE_EXECUTE_STANDARDTIME[service_id],
                                4,
                            )
                            node_qos_dict[node][service_id]["slackScore"] = slackScore
                            if slackScore < threshold_a:
                                if (
                                    slackScore
                                    < node_qos_dict[node][service_id]["min_slack"]
                                ):
                                    node_qos_dict[node][service_id][
                                        "min_slack"
                                    ] = slackScore
                                if MY_QOS_SERVICE_DICT[node][service_id] < max_range:
                                    if MY_QOS_SERVICE_DICT[node][service_id] > 0:
                                        MY_QOS_SERVICE_DICT[node][
                                            service_id
                                        ] += per_cpu_add
                                    else:
                                        init_recovery = (
                                            (max_range - 0)
                                            * (threshold_a - slackScore)
                                            / (
                                                threshold_a
                                                - node_qos_dict[node][service_id][
                                                    "min_slack"
                                                ]
                                            )
                                        )
                                        MY_QOS_SERVICE_DICT[node][
                                            service_id
                                        ] = init_recovery
                            elif slackScore > threshold_b:
                                if MY_QOS_SERVICE_DICT[node][service_id] > min_range:
                                    MY_QOS_SERVICE_DICT[node][service_id] -= per_cpu_sub
                            else:
                                pass
                        else:
                            continue
                    except Exception as e:
                        pass
            count_noZero = 0
            degree_sum = 0
            for node in node_qos_dict:
                for service_id in node_qos_dict[node]:
                    if node_qos_dict[node][service_id]["slackScore"] != 0:
                        count_noZero += 1
                        degree_sum += node_qos_dict[node][service_id]["slackScore"]
                    with thr_lock:
                        node_qos_dict[node][service_id]["time_list"].clear()
                        node_qos_dict[node][service_id]["slackScore"] = 0
            count_change = 0
            change_sum = 0
            my_qos_service_dict = valueclone_nested_dict_proxy(MY_QOS_SERVICE_DICT)
            for node in my_qos_service_dict:
                change_sum += sum(my_qos_service_dict[node].values())
                count_change += len(my_qos_service_dict[node].values())
            if count_noZero == 0:
                avg_dgree = 0
            else:
                avg_dgree = round(degree_sum / count_noZero, 4)
            avg_change = round(change_sum / count_change, 4)
            record_list = [avg_dgree, avg_change]

            with open("./qos_record.csv", "a+", newline="") as f:
                csv_write = csv.writer(f)
                csv_write.writerow(record_list)

            time.sleep(time_windows)
        else:
            time.sleep(1)


def LC_QOS_info_excute(lc_qos_info_queue, MY_QOS_SERVICE_DICT):
    """
    LC QoS detector function
    """
    node_qos_dict = {}
    start_counter = [0]
    for node in EDGEMASTER_IP_DICTSET[master_name]["nodeset"]:
        node_qos_dict[node] = {}
        for lc_service_id in LC_SERVICE_LIST:
            node_qos_dict[node][lc_service_id] = {
                "time_list": [],
                "min_slack": -0.7,
                "slackScore": 0,
            }
    # print("node_qos_dict:", node_qos_dict)

    thr_lock = threading.Lock()
    thr_check_policy = threading.Thread(
        target=THR_check_LC_policy,
        args=(node_qos_dict, thr_lock, start_counter, MY_QOS_SERVICE_DICT),
    )
    thr_check_policy.start()

    while True:
        qos_info = lc_qos_info_queue.get()
        target_node, service_id, useTime = qos_info[0], str(qos_info[1]), qos_info[2]
        start_counter[0] += 1
        # print("start_counter[0]:", start_counter[0])
        # print("qos_info:", qos_info)
        if useTime > 120:
            with thr_lock:
                node_qos_dict[target_node][service_id]["time_list"].append(useTime)


def THR_edge_commuicate_info_update(pool_args):
    (
        edge_info_dict,
        MY_CLUSTER_DICT,
        myCLUSTER_lock_dict,
        MY_MEO_DICT,
        MY_CPU_DICT,
        MY_LOAD_DICT,
    ) = pool_args
    edge_info_dict = json.loads(edge_info_dict.decode("utf-8"))
    for master in edge_info_dict:
        for node in edge_info_dict[master]:
            with myCLUSTER_lock_dict[master][node]:
                MY_CLUSTER_DICT[master][node]["cpu"] = edge_info_dict[master][node][
                    "cpu"
                ]
                MY_CLUSTER_DICT[master][node]["meo"] = edge_info_dict[master][node][
                    "meo"
                ]
                MY_CLUSTER_DICT[master][node]["load"] = edge_info_dict[master][node][
                    "load"
                ]

    tmp_local_cluster = valueclone_nested_dict_proxy(MY_CLUSTER_DICT)
    my_cpu_dict = valueclone_nested_dict_proxy(MY_CPU_DICT)
    my_meo_dict = valueclone_nested_dict_proxy(MY_MEO_DICT)
    for node in tmp_local_cluster[master_name]:
        with myCLUSTER_lock_dict[master_name][node]:
            MY_CLUSTER_DICT[master_name][node]["cpu"] = my_cpu_dict[node]
            MY_CLUSTER_DICT[master_name][node]["meo"] = my_meo_dict[node]
        MY_CLUSTER_DICT[master_name][node]["load"] = MY_LOAD_DICT[node]


def edge_commuicate_info_update(
    edge_communicate_info_queue,
    MY_CLUSTER_DICT,
    myCLUSTER_lock_dict,
    MY_MEO_DICT,
    MY_CPU_DICT,
    MY_LOAD_DICT,
):
    count = 0
    pool = ThreadPoolExecutor()
    while True:
        edge_info_dict = edge_communicate_info_queue.get()
        task = pool.submit(
            THR_edge_commuicate_info_update,
            (
                edge_info_dict,
                MY_CLUSTER_DICT,
                myCLUSTER_lock_dict,
                MY_MEO_DICT,
                MY_CPU_DICT,
                MY_LOAD_DICT,
            ),
        )
        task.add_done_callback(thread_pool_callback)
        count += 1


def edge_commuicate_info_get(edge_communicate_info_queue):
    """
    state storage function
    """

    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", EDGE_INFO_SYNERGY_UPDATE_PORT))
    edge_master_receive_server.listen(200)

    while True:
        s1, addr = edge_master_receive_server.accept()
        lengthData = s1.recv(6)
        length = int.from_bytes(lengthData, byteorder="big")
        if length == 0:
            continue
        content = bytes()
        count = 0
        while True:
            value = s1.recv(length)
            content = content + value
            count += len(value)
            if count >= length:
                break
        edge_communicate_info_queue.put(content)
        s1.close()


def edge_commuicate_info_send(MY_MEO_DICT, MY_CPU_DICT, MY_LOAD_DICT):
    my_send_dict = {master_name: {}}
    for node in EDGEMASTER_IP_DICTSET[master_name]["nodeset"]:
        my_send_dict[master_name][node] = {}

    while True:
        # with pro_share_lock8, pro_share_lock9, pro_share_lock10:
        MY_CPU_DICT_tmp = valueclone_nested_dict_proxy(MY_CPU_DICT)
        MY_MEO_DICT_tmp = valueclone_nested_dict_proxy(MY_MEO_DICT)
        # MY_LOAD_DICT_tmp = valueclone_nested_dict_proxy(MY_LOAD_DICT)
        for each_node in my_send_dict[master_name]:
            my_send_dict[master_name][each_node]["cpu"] = MY_CPU_DICT_tmp[each_node]
            my_send_dict[master_name][each_node]["meo"] = MY_MEO_DICT_tmp[each_node]
            # my_send_dict[master_name][each_node]["load"] = MY_LOAD_DICT_tmp[each_node]
        my_cluster_info = json.dumps(my_send_dict)

        for master in EDGEMASTER_IP_DICTSET:
            if master != master_name:
                now_target_ip = EDGEMASTER_IP_DICTSET[master]["IP"]
                try:
                    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # print("now_target_ip:", now_target_ip)
                    client.connect((now_target_ip, EDGE_INFO_SYNERGY_UPDATE_PORT))
                    client.send(
                        len(my_cluster_info).to_bytes(length=6, byteorder="big")
                    )
                    client.sendall(bytes(my_cluster_info.encode("utf-8")))
                    client.close()
                except Exception as e:
                    pass
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((CENTRAL_MASTER_IP, COLLECT_CLUSTER_INFO))
        client.send(len(my_cluster_info).to_bytes(length=6, byteorder="big"))
        client.sendall(bytes(my_cluster_info.encode("utf-8")))
        client.close()

        time.sleep(0.1)


def node_load__get(MY_LOAD_DICT, pro_share_lock10):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", EDGE_NODE_LOAD_UPDATE_PORT))
    edge_master_receive_server.listen(100)

    while True:
        s1, addr = edge_master_receive_server.accept()
        raw_data = s1.recv(2048)
        edge_load_list = json.loads(raw_data.decode("utf-8"))
        node_name = edge_load_list[0]
        load = float(edge_load_list[1])
        with pro_share_lock10:
            MY_LOAD_DICT[node_name] = load
        s1.close()


def pod_list_get(
    MY_PODLIST_DICT,
):
    check_service_id_list = LC_SERVICE_LIST.copy()
    # In our scenario the BE services are co-located in the same container (which acts as a base environment).
    check_service_id_list.append("0")
    tmp_list = []

    # Complete the initialization of all services (where BE services are written in the same container)
    while len(tmp_list) < len(check_service_id_list):
        tmp_list = []
        for each_id in check_service_id_list:
            service_name = "service" + each_id
            pod_list = check_pod(service_name)
            if len(pod_list) >= len(check_service_id_list):
                tmp_list.append(pod_list)
            else:
                break
        time.sleep(3)
        logger.info("Waiting for all setting containers to be initialized...")

    for i in range(len(check_service_id_list)):
        MY_PODLIST_DICT[check_service_id_list[i]] = tmp_list[i]
    logger.info(str(valueclone_nested_dict_proxy(MY_PODLIST_DICT)))


def run_lc_single(
    lc_run_queue,
    MY_MEO_DICT,
    myMEO_lock_dict,
    MY_CPU_DICT,
    myCPU_lock_dict,
    TEST_CPU_MEO_lock_dict,
    TEST_MY_CPU_MEO_DICT,
):
    pool = ThreadPoolExecutor()

    while True:
        info_run_req = lc_run_queue.get()
        pod_IP, req, conta_id = info_run_req[0], info_run_req[1], info_run_req[2]
        task = pool.submit(
            run_run_run,
            (
                pod_IP,
                req,
                conta_id,
                MY_CPU_DICT,
                myCPU_lock_dict,
                myMEO_lock_dict,
                MY_MEO_DICT,
                "",
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )


def run_be_single(
    be_run_queue,
    MY_MEO_DICT,
    myMEO_lock_dict,
    MY_CPU_DICT,
    myCPU_lock_dict,
    tmp_write_lock,
    TEST_CPU_MEO_lock_dict,
    TEST_MY_CPU_MEO_DICT,
):
    pool = ThreadPoolExecutor()
    # tmp_write_lock = threading.Lock()

    while True:
        info_run_req = be_run_queue.get()
        pod_IP, req = info_run_req[0], info_run_req[1]
        task = pool.submit(
            run_run_run,
            (
                pod_IP,
                req,
                "",
                MY_CPU_DICT,
                myCPU_lock_dict,
                myMEO_lock_dict,
                MY_MEO_DICT,
                tmp_write_lock,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        task.add_done_callback(thread_pool_callback)


def init_min_max_info():
    global start_nodes, end_nodes, unit_costs, index_dict
    node_num = 0
    start_nodes_copy = []
    end_nodes_copy = []
    unit_costs = []
    index_dict = {}

    for master in EDGEMASTER_IP_DICTSET:
        index_dict[master] = node_num
        node_num += 1
        for node in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
            start_nodes_copy.append(master)
            end_nodes_copy.append(node)
            index_dict[node] = node_num
            per_cost = (
                int(
                    distEclud(
                        np.array(node_position[master]), np.array(node_position[node])
                    )
                )
                + node_trans_property[master]
                + node_trans_property[node]
            )
            unit_costs.append(per_cost)
            node_num += 1

    for master in EDGEMASTER_IP_DICTSET:
        for master_2 in EDGEMASTER_IP_DICTSET:
            if master != master_2:
                start_nodes_copy.append(master)
                end_nodes_copy.append(master_2)
                per_cost = (
                    int(
                        distEclud(
                            np.array(node_position[master]),
                            np.array(node_position[master_2]),
                        )
                    )
                    + node_trans_property[master]
                    + node_trans_property[master_2]
                )
                unit_costs.append(per_cost)

    start_nodes = []
    end_nodes = []
    for i in range(len(start_nodes_copy)):
        start_nodes.append(index_dict[start_nodes_copy[i]])
        end_nodes.append(index_dict[end_nodes_copy[i]])
    # return start_nodes, end_nodes, unit_costs, index_dict


def receive_lc_result(lc_result_queue):
    edge_master_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    edge_master_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    edge_master_receive_server.bind(("0.0.0.0", LC_RESULT_PORT))
    edge_master_receive_server.listen(1000)
    while True:
        s1, addr = edge_master_receive_server.accept()
        req_result = s1.recv(1024 * 1)
        req_result = json.loads(req_result.decode("utf-8"))
        lc_result_queue.put(req_result)
        s1.close()


def result_qos_send(req, useTime, runtime):

    lc_SLO_slackDgree = round(
        (runtime - ALL_SERVICE_EXECUTE_STANDARDTIME[str(req[0])])
        / ALL_SERVICE_EXECUTE_STANDARDTIME[str(req[0])],
        5,
    )
    lc_dict = {
        "LCtype": req[0],
        "node": req[-1],
        "t1": delay_user_edge_simple * 2,
        "t2": useTime,
        "all_t": runtime,
        "slackScore": lc_SLO_slackDgree,
        "time_get": time.time(),
    }

    lc_dict = json.dumps(lc_dict)
    try:
        client3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client3.connect(("0.0.0.0", COLLECT_LC_INFO))
        client3.sendall(bytes(lc_dict.encode("utf-8")))
        client3.close()
    except Exception as e:
        pass


def thr_lc_result_execute(pool_args):
    (
        req_result,
        MY_MEO_DICT,
        myMEO_lock_dict,
        MY_CPU_DICT,
        myCPU_lock_dict,
        tmp_write_lock,
        lc_qos_info_queue,
        MY_PODLIST_DICT,
        TEST_CPU_MEO_lock_dict,
        TEST_MY_CPU_MEO_DICT,
    ) = pool_args

    req, useTime, node_get_time = req_result[0], req_result[1], req_result[2]
    if DVPA_REGU_bool:
        with myMEO_lock_dict[req[-1]]["LC_MEO"]:
            security_meo_back(req, MY_MEO_DICT)
        with myCPU_lock_dict[req[-1]]["LC_CPU_SH"]:
            security_cpu_back(req, MY_CPU_DICT)
    else:
        cpu_should = LC_CPU_ONCE_NEED_DICT[str(req[0])][0] + req[2]
        meo_need = LC_MEO_ONCE_NEED_DICT[str(req[0])]
        with TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])][
            "CPU_SH"
        ], TEST_CPU_MEO_lock_dict[req[-1]][str(req[0])]["MEO"]:
            TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_CPU"] += cpu_should
            TEST_MY_CPU_MEO_DICT[req[-1]][str(req[0])]["AVAI_MEO"] += meo_need

    distance = (
        float(
            distEclud(
                np.array(node_position[master_name]), np.array(node_position[req[3]])
            )
        )
        + node_trans_property[master_name]
        + node_trans_property[req[3]]
    )

    total_time = time.time() - req[1]

    if QOS_REASS_bool:
        qos_info = [req[-1], req[0], useTime]
        lc_qos_info_queue.put(qos_info)

    if total_time <= ALL_SERVICE_EXECUTE_STANDARDTIME[str(req[0])]:
        detail_mission = {
            "masterName": master_name,
            "nodeName": req[-1],
            "service_id": req[0],
            "success": 1,
            "stuck": 0,
            "failure": 0,
        }
    else:
        detail_mission = {
            "masterName": master_name,
            "nodeName": req[-1],
            "service_id": req[0],
            "success": 0,
            "stuck": 0,
            "failure": 1,
        }

    if cloud_each_task_situation:
        this_mission2 = json.dumps(detail_mission)
        client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client2.connect((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
        client2.sendall(bytes(this_mission2.encode("utf-8")))
        client2.close()

    tmp_list = [
        req[0],
        req[-2],
        req[1],
        req[3],
        req[4],
        req[2],
        req[-1],
        distance,
        total_time,
    ]

    if cloud_each_lcbe_record:
        tmp_list = json.dumps(tmp_list)
        this_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        this_client.connect((CENTRAL_MASTER_IP, CLOUD_RESULT_PORT))
        this_client.sendall(bytes(tmp_list.encode("utf-8")))
        this_client.close()


def excute_lc_result(
    lc_result_queue,
    MY_MEO_DICT,
    pro_share_lock8,
    MY_CPU_DICT,
    pro_share_lock9,
    tmp_write_lock,
    lc_qos_info_queue,
    MY_PODLIST_DICT,
    TEST_CPU_MEO_lock_dict,
    TEST_MY_CPU_MEO_DICT,
):
    # tmp_write_lock = threading.Lock()
    pool = ThreadPoolExecutor()
    while True:
        req_result = lc_result_queue.get()
        task = pool.submit(
            thr_lc_result_execute,
            (
                req_result,
                MY_MEO_DICT,
                myMEO_lock_dict,
                MY_CPU_DICT,
                myCPU_lock_dict,
                tmp_write_lock,
                lc_qos_info_queue,
                MY_PODLIST_DICT,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        task.add_done_callback(thread_pool_callback)


if __name__ == "__main__":
    # modify_buff_size()
    init_min_max_info()
    node_list = getNodeName()

    orchestra_queue = Queue(maxsize=100)  # Service Orchestration Queue
    edge_communicate_info_queue = Queue(maxsize=1000)
    lc_qos_info_queue = Queue(maxsize=500)

    cache_queue = Queue(maxsize=500)  # LC Pending Decision Queue
    lc_excute_queue = Queue(maxsize=500)  # LC Pending Queue
    lc_run_queue = Queue(maxsize=1000)  # LC Pending RUN Queue
    lc_result_queue = Queue(maxsize=1000)  # LC Pending RUN Queue

    be_queue = Queue(maxsize=500)  # BE Pending Decision Queue
    be_excute_queue = Queue(maxsize=1000)  # BE Pending Queue
    be_run_queue = Queue(maxsize=1000)  # BE pending RUN queue

    manager = Manager()
    NODE_LC_DICT = manager.dict()
    MY_MEO_DICT = manager.dict()
    MY_CPU_DICT = manager.dict()
    MY_CLUSTER_DICT = manager.dict()
    MY_LOAD_DICT = manager.dict()
    MY_PODLIST_DICT = manager.dict()
    MY_QOS_SERVICE_DICT = manager.dict()

    TEST_CPU_MEO_lock_dict = {}
    TEST_MY_CPU_MEO_DICT = manager.dict()

    myMEO_lock_dict = {}
    myCPU_lock_dict = {}
    node_num = 4
    for node in node_list:
        MY_MEO_DICT[node] = manager.dict(
            {
                "LC_MEO": 0,
                "BE_MEO": 0,
                "FREE_MEO": MEO_TOTAL_INIT - CONTAINER_INIT_MEO["0"],
                "CON_MEO": CONTAINER_INIT_MEO["0"],
            }
        )
        MY_CPU_DICT[node] = manager.dict(
            {"LC_CPU_SH": 0, "BE_CPU_SH": 0, "TOTAL_INIT": CPU_TOTAL_INIT}
        )
        MY_LOAD_DICT[node] = 0
        myMEO_lock_dict[node] = {
            "LC_MEO": multiprocessing.Lock(),
            "BE_MEO": multiprocessing.Lock(),
        }
        myCPU_lock_dict[node] = {
            "LC_CPU_SH": multiprocessing.Lock(),
            "BE_CPU_SH": multiprocessing.Lock(),
        }
        MY_QOS_SERVICE_DICT[node] = manager.dict()

        TEST_CPU_MEO_lock_dict[node] = {}
        TEST_MY_CPU_MEO_DICT[node] = manager.dict()

        for service_id in LC_SERVICE_LIST:
            MY_QOS_SERVICE_DICT[node][service_id] = 0

            TEST_CPU_MEO_lock_dict[node][service_id] = {
                "CPU_SH": multiprocessing.Lock(),
                "MEO": multiprocessing.Lock(),
            }
            TEST_MY_CPU_MEO_DICT[node][service_id] = manager.dict(
                {
                    "AVAI_CPU": CPU_TOTAL_INIT / node_num,
                    "AVAI_MEO": MEO_TOTAL_INIT / node_num,
                }
            )

        TEST_CPU_MEO_lock_dict[node]["0"] = {
            "CPU_SH": multiprocessing.Lock(),
            "MEO": multiprocessing.Lock(),
        }
        TEST_MY_CPU_MEO_DICT[node]["0"] = manager.dict(
            {
                "AVAI_CPU": CPU_TOTAL_INIT / node_num,
                "AVAI_MEO": MEO_TOTAL_INIT / node_num,
            }
        )

    myCLUSTER_lock_dict = {}
    for master in EDGEMASTER_IP_DICTSET:
        MY_CLUSTER_DICT[master] = manager.dict()
        myCLUSTER_lock_dict[master] = {}
        for node in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
            myCLUSTER_lock_dict[master][node] = multiprocessing.Lock()
            MY_CLUSTER_DICT[master][node] = manager.dict()
            MY_CLUSTER_DICT[master][node]["cpu"] = manager.dict(
                {"LC_CPU_SH": 0, "BE_CPU_SH": 0, "TOTAL_INIT": CPU_TOTAL_INIT}
            )
            MY_CLUSTER_DICT[master][node]["meo"] = manager.dict(
                {
                    "LC_MEO": 0,
                    "BE_MEO": 0,
                    "FREE_MEO": MEO_TOTAL_INIT - CONTAINER_INIT_MEO["0"],
                    "CON_MEO": CONTAINER_INIT_MEO["0"],
                }
            )
            MY_CLUSTER_DICT[master][node]["load"] = 0

    pro_share_lock9 = multiprocessing.Lock()
    pro_share_lock8 = multiprocessing.Lock()
    pro_share_lock6 = multiprocessing.Lock()
    # pro_share_lock1 = multiprocessing.Lock()
    pro_share_lock10 = multiprocessing.Lock()
    tmp_write_lock = multiprocessing.Lock()

    # Get the data of LOAD on worker node
    # process_r = multiprocessing.Process(target=node_load__get, args=(MY_LOAD_DICT, pro_share_lock10))
    # process_r.start()

    # Get information about container resources on worker
    process_r_docker = multiprocessing.Process(
        target=node_container_get_send, args=(MY_CPU_DICT,)
    )
    process_r_docker.start()

    # Get resource usage from other edge clusters and synchronize data with information from this cluster
    process_r = multiprocessing.Process(
        target=edge_commuicate_info_get, args=(edge_communicate_info_queue,)
    )
    process_r.start()

    for i in range(2):
        process_r = multiprocessing.Process(
            target=edge_commuicate_info_update,
            args=(
                edge_communicate_info_queue,
                MY_CLUSTER_DICT,
                myCLUSTER_lock_dict,
                MY_MEO_DICT,
                MY_CPU_DICT,
                MY_LOAD_DICT,
            ),
        )
        process_r.start()

    # LC service orchestration (receiving and execution)
    process_LC_orchestra_receive = multiprocessing.Process(
        target=LC_orchestra_receive, args=(orchestra_queue,)
    )
    process_LC_orchestra_receive.start()
    process_LC_orchestra_execute = multiprocessing.Process(
        target=LC_orchestra_execute,
        args=(orchestra_queue, MY_MEO_DICT, pro_share_lock8),
    )
    process_LC_orchestra_execute.start()

    # LC QOS Re-assurance Strategy
    process_LC_QOS_info_excute = multiprocessing.Process(
        target=LC_QOS_info_excute, args=(lc_qos_info_queue, MY_QOS_SERVICE_DICT)
    )
    process_LC_QOS_info_excute.start()

    # process_LCRecivMind = multiprocessing.Process(target=core_LCRecivMind,args=(NODE_LC_DICT, pro_share_lock6))
    # process_LCRecivMind.start()

    # The rest of the Cluster messages are sent
    # process_cluster = multiprocessing.Process(target=cluster_Send,args=(NODE_meo_DICT, pro_share_lock7, NODE_CPU_DICT,pro_share_lock2, NODE_load_DICT, pro_share_lock3,
    # pro_share_lock6, NODE_LC_DICT))
    # process_cluster.start()

    # Edge cluster SH resource occupancy is synchronized to the central cluster
    if RES_SITUATION_SEND:
        pro_edge_cluster_send = multiprocessing.Process(
            target=edge_commuicate_info_send,
            args=(MY_MEO_DICT, MY_CPU_DICT, MY_LOAD_DICT),
        )
        pro_edge_cluster_send.start()

    # ----------------------------------------------------------------------------------------------------------------------

    # LC Pending Decision Listening
    process_receive_request_from_center = multiprocessing.Process(
        target=receive_request_from_center, args=(cache_queue,)
    )
    process_receive_request_from_center.start()

    # LC decision processing
    for i in range(1):
        process_execute_request_from_center = multiprocessing.Process(
            target=execute_request_from_center,
            args=(cache_queue, MY_MEO_DICT, MY_CPU_DICT, MY_CLUSTER_DICT),
        )
        process_execute_request_from_center.start()

    # LC pending execution listener (execution requests from other clusters)
    process_receive_request_from_other = multiprocessing.Process(
        target=receive_lc_from_other, args=(lc_excute_queue,)
    )
    process_receive_request_from_other.start()

    # LC execution processing
    for i in range(1):
        process_execute_request_lc = multiprocessing.Process(
            target=run_request_lc,
            args=(
                lc_excute_queue,
                MY_MEO_DICT,
                myMEO_lock_dict,
                MY_CPU_DICT,
                myCPU_lock_dict,
                MY_PODLIST_DICT,
                lc_run_queue,
                MY_QOS_SERVICE_DICT,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        process_execute_request_lc.start()

    # LC RUN processing
    for i in range(1):
        process_run_request_lc = multiprocessing.Process(
            target=run_lc_single,
            args=(
                lc_run_queue,
                MY_MEO_DICT,
                myMEO_lock_dict,
                MY_CPU_DICT,
                myCPU_lock_dict,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        process_run_request_lc.start()

    # LC Receive processing results
    process_receive_result_from_node = multiprocessing.Process(
        target=receive_lc_result, args=(lc_result_queue,)
    )
    process_receive_result_from_node.start()

    # # LC statistical processing results
    for i in range(3):
        process_receive_result_from_node = multiprocessing.Process(
            target=excute_lc_result,
            args=(
                lc_result_queue,
                MY_MEO_DICT,
                myMEO_lock_dict,
                MY_CPU_DICT,
                myCPU_lock_dict,
                tmp_write_lock,
                lc_qos_info_queue,
                MY_PODLIST_DICT,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        process_receive_result_from_node.start()

    # ----------------------------------------------------------------------------------------------------------------------
    # BE pending decision listening (should be sent to the central cluster if the decision maker is the central dispatch)
    process_receive_from_center = multiprocessing.Process(
        target=BEreceive_receive_from_center, args=(be_queue,)
    )
    process_receive_from_center.start()

    # BE decision processing
    for i in range(2):
        process_execute_be = multiprocessing.Process(
            target=BEreceive_execute_from_center,
            args=(be_queue, MY_MEO_DICT, MY_CPU_DICT, MY_PODLIST_DICT, MY_CLUSTER_DICT),
        )
        process_execute_be.start()

    # BE listening and execution processes, differentiating LC logic (execution requests from other clusters)
    process_receive_request_from_other = multiprocessing.Process(
        target=receive_be_from_other,
        args=(
            MY_MEO_DICT,
            myMEO_lock_dict,
            MY_CPU_DICT,
            myCPU_lock_dict,
            MY_PODLIST_DICT,
            be_run_queue,
            TEST_CPU_MEO_lock_dict,
            TEST_MY_CPU_MEO_DICT,
        ),
    )
    process_receive_request_from_other.start()

    # BE execution processing
    # for i in range(1):
    # process_execute_request_from_be = multiprocessing.Process(target=run_request_be,
    # args=(be_excute_queue, MY_MEO_DICT, myMEO_lock_dict, MY_CPU_DICT, myCPU_lock_dict, MY_PODLIST_DICT, be_run_queue))
    # process_execute_request_from_be.start()

    # BE RUN processing
    for i in range(2):
        process_run_request_be = multiprocessing.Process(
            target=run_be_single,
            args=(
                be_run_queue,
                MY_MEO_DICT,
                myMEO_lock_dict,
                MY_CPU_DICT,
                myCPU_lock_dict,
                tmp_write_lock,
                TEST_CPU_MEO_lock_dict,
                TEST_MY_CPU_MEO_DICT,
            ),
        )
        process_run_request_be.start()
    # ----------------------------------------------------------------------------------------------------------------------

    # Get the data of POD_LIST
    process_r = multiprocessing.Process(target=pod_list_get, args=(MY_PODLIST_DICT,))
    process_r.start()

    process_r_docker.join()
