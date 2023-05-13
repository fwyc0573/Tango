import json
import os
import pickle
import socket
import csv
import time
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from .checkPod import Pod
import torch
from .config.config_port import *
from .config.config_service import *
from .config.config_others import *


def init_Graph_data_x():
    global start_nodes, end_nodes, master_index_dict, master_index_dict_rev, final_list
    start_nodes, end_nodes, master_index_dict, master_index_dict_rev, num_id_now = (
        [],
        [],
        {},
        {},
        0,
    )
    for master in EDGEMASTER_IP_DICTSET:
        for node in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
            start_nodes.append(master)
            end_nodes.append(node)
    master_list = list(EDGEMASTER_IP_DICTSET.keys())
    for i in range(len(master_list)):
        now_master = master_list[i]
        if i != len(master_list) - 1:
            for j in range(i + 1, len(master_list)):
                start_nodes.append(now_master)
                end_nodes.append(master_list[j])
    start_nodes_copy = start_nodes.copy()
    end_nodes_copy = end_nodes.copy()
    start_nodes += end_nodes_copy
    end_nodes += start_nodes_copy

    # string dict
    final_list = [start_nodes, end_nodes]

    # Generate the mapping dictionary index_dict
    for per_list in final_list:
        for each_node in per_list:
            if each_node not in master_index_dict:
                master_index_dict[each_node] = num_id_now
                num_id_now += 1

    for per_list in final_list:
        for i in range(len(per_list)):
            per_list[i] = master_index_dict[per_list[i]]

    master_index_dict_rev = dict([val, key] for key, val in master_index_dict.items())


def init_mask_tensor():
    global master_index_dict_rev
    mask_list_add = []
    mask_list_mul = []
    pure_node_list = []

    for index_node in master_index_dict_rev:
        if master_index_dict_rev[index_node] in EDGEMASTER_IP_DICTSET:
            mask_list_add.append(0.00001)
        else:
            mask_list_add.append(0)
            pure_node_list.append(master_index_dict_rev[index_node])

    for index_node in master_index_dict_rev:
        if master_index_dict_rev[index_node] in EDGEMASTER_IP_DICTSET:
            mask_list_mul.append(0)
        else:
            mask_list_mul.append(1)

    return torch.tensor(mask_list_add), torch.tensor(mask_list_mul), pure_node_list


def get_observation(
    req,
    tmp_resources_on_cluster,
    tmp_tasks_execute_situation_on_each_node,
    tmp_resources_on_each_node_dict,
):
    global master_index_dict, master_index_dict_rev
    obs = []
    index_req = str(req[0])
    req_num = req[-3]
    BE_load = int(BE_LOAD_DICT[index_req]["cpu"]) * req_num
    BE_cpu = int(int(BE_CPU_ONCE_NEED_DICT[index_req]) * req_num / 100)
    BE_meo = int(BE_LOAD_DICT[index_req]["mem"]) * req_num

    for i in range(len(master_index_dict_rev)):
        node_name = master_index_dict_rev[i]
        if node_name in EDGEMASTER_IP_DICTSET:  # i.e., master node
            just_need = [BE_cpu, BE_meo, BE_load]
            obs.append([0] * Effective_WORKER_FEATURE + just_need)
        else:
            for master in EDGEMASTER_IP_DICTSET:
                if node_name in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
                    map_master = master
                    break

            LC_CPU_SH = tmp_resources_on_cluster[map_master][node_name]["cpu"][
                "LC_CPU_SH"
            ]
            BE_CPU_SH = tmp_resources_on_cluster[map_master][node_name]["cpu"][
                "BE_CPU_SH"
            ]
            TOTAL_INIT = tmp_resources_on_cluster[map_master][node_name]["cpu"][
                "TOTAL_INIT"
            ]
            FREE_BE_CPU_SH = TOTAL_INIT - BE_CPU_SH - LC_CPU_SH
            LC_MEO = tmp_resources_on_cluster[map_master][node_name]["meo"]["LC_MEO"]
            BE_MEO = tmp_resources_on_cluster[map_master][node_name]["meo"]["BE_MEO"]
            FREE_MEO = tmp_resources_on_cluster[map_master][node_name]["meo"][
                "FREE_MEO"
            ]
            REAL_LC_CPU = 0
            REAL_BE_CPU = 0

            for service_id in tmp_resources_on_each_node_dict[map_master][node_name]:
                if str(service_id) in LC_SERVICE_LIST:
                    REAL_LC_CPU += tmp_resources_on_each_node_dict[map_master][
                        node_name
                    ][service_id]["cpu_use"]
                else:
                    REAL_BE_CPU += tmp_resources_on_each_node_dict[map_master][
                        node_name
                    ][service_id]["cpu_use"]

            res_cpu_need_sum = 0
            for service_id in tmp_tasks_execute_situation_on_each_node[map_master][
                node_name
            ]:
                if service_id not in LC_SERVICE_LIST:
                    res_cpu_need_sum += (
                        tmp_tasks_execute_situation_on_each_node[map_master][node_name][
                            service_id
                        ]["ST"]
                        * BE_CPU_ONCE_NEED_DICT[service_id]
                    )

            just_need = [
                LC_CPU_SH / 100,
                BE_CPU_SH / 100,
                FREE_BE_CPU_SH / 100,
                LC_MEO,
                BE_MEO,
                FREE_MEO,
                REAL_LC_CPU / 100,
                REAL_BE_CPU / 100,
                res_cpu_need_sum / 100,
            ]
            obs.append(just_need + [0] * Effective_MASTER_FEATURE)
    return obs


def thread_pool_callback(worker):
    worker_exception = worker.exception()
    if worker_exception:
        logger.exception("error".format(worker_exception))


def receive_request_from_edge(cache_queue):
    dispatch_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dispatch_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    dispatch_receive_server.bind(("0.0.0.0", DISPATCH_RECEIVE_REQUEST_PORT))
    dispatch_receive_server.listen(1000)
    while True:
        s1, addr = dispatch_receive_server.accept()
        length_data = s1.recv(1024)
        if length_data:
            this_request = bytes() + length_data
            request = pickle.loads(this_request)
            cache_queue.put(request)
            s1.close()


def clear_all_dict(*dict_proxies):
    for dict_proxy in dict_proxies:
        # if node in dict_proxy:
        for key, value in dict_proxy.items():
            dict_proxy[key].clear()


def task_reward_get(reward_back_queue):
    dispatch_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dispatch_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    dispatch_receive_server.bind(("0.0.0.0", BE_REWARD_PORT))
    dispatch_receive_server.listen(1000)
    while True:
        s1, addr = dispatch_receive_server.accept()
        req_reward_back = s1.recv(1024)
        req_reward_back = json.loads(req_reward_back.decode("utf-8"))

        task_id, reward = req_reward_back[0], req_reward_back[1]
        reward_back_queue.put([task_id, reward].copy())
        s1.close()


def task_dict_update(task_info_queue, task_info_dict, manager):
    while True:
        task_info = task_info_queue.get()
        task_id, task_num, task_action = task_info[0], int(task_info[1]), task_info[2]
        task_info_dict[task_id] = manager.dict({"num": task_num, "ac": task_action})
        logger.info("we got a new task：", valueclone_nested_dict_proxy(task_info_dict))


def THR_meomery_push(
    manager_resources_on_cluster,
    manager_task_dict,
    manager_resources_on_each_node_dict,
    memory,
    reward_back_queue,
    total_numsteps,
    dict_lock,
    meomery_dict,
    memory_lock,
):
    record_delet_id = 0
    reward_record = []
    while True:
        if reward_back_queue.qsize() > 10:
            for i in range(10):
                reward_back_list = reward_back_queue.get()
                task_id, reward = str(reward_back_list[0]), float(reward_back_list[1])
                try:
                    req, action, state, mask = (
                        meomery_dict[task_id]["req"],
                        meomery_dict[task_id]["ac"],
                        meomery_dict[task_id]["st"],
                        meomery_dict[task_id]["ma"],
                    )

                    next_state = get_observation(
                        req,
                        manager_resources_on_cluster,
                        manager_task_dict,
                        manager_resources_on_each_node_dict,
                    )
                    next_state = np.array(next_state)
                    with memory_lock:
                        memory.push(state, action, reward, next_state, mask)
                    reward_record.append(reward)
                except Exception as e:
                    pass

            if len(reward_record) != 0 and len(reward_record) % 3 == 0:
                reward_avg = sum(reward_record) / len(reward_record)
                record_list = [total_numsteps[0], reward_avg]
                with open("./sac_reward.csv", "a+", newline="") as f:
                    csv_write = csv.writer(f)
                    csv_write.writerow(record_list)
                reward_record.clear()

            with dict_lock:
                if len(meomery_dict) > 200:
                    for i in range(record_delet_id, record_delet_id + 100):
                        index = str(i)
                        meomery_dict.pop(index, None)
                    record_delet_id += 100
        else:
            time.sleep(2)


def centralized_scheduling_dispatcher(
    cache_queue,
    edge_queue_dict,
    manager_resources_on_cluster,
    manager_task_dict,
    manager_resources_on_each_node_dict,
    reward_back_queue,
    tmp_lock,
):
    """
    BE dispatcher function
    We have deployed DCG in the central cluster of the simulation environment,
    and here we give an example (SAC) to demonstrate how central scheduling is
    implemented in the central master of a realistic cluster. For more information
    about DCG's model, please refer to model_dcg.py. You can place a custom
    central scheduling policy at this function interface.
    """

    from .examle_sac.sac import SAC
    from .replay_memory import ReplayMemory
    from gym import spaces

    task_id = 0
    init_Graph_data_x()
    global start_nodes, end_nodes, master_index_dict, master_index_dict_rev, final_list, mask_tensor_add, mask_tensor_mul
    mask_tensor_add, mask_tensor_mul, pure_node_list = init_mask_tensor()

    my_action_space = spaces.Box(low=0.01, high=1, shape=(1,), dtype=np.float32)
    my_action_space_sample = spaces.Box(
        low=0.01, high=1, shape=(len(master_index_dict), 1), dtype=np.float32
    )
    mask_np = mask_tensor_mul.cpu().numpy().copy()
    from .examle_sac.sac_args import args

    agent = SAC(FEATURE_SIZE, my_action_space, args, final_list)

    # agent.load_checkpoint("./checkpoints/sac_checkpoint_sac_")
    memory = ReplayMemory(args.replay_size, args.seed)
    total_numsteps = [0]
    updates = 0
    reward_record = []
    record_sum = 0
    train_model = False
    memory_lock = threading.Lock()

    while True:
        req = cache_queue.get()
        total_num = req[-3]
        still_do = True

        # Scheduling decision every 10 requests of the same type
        while still_do:
            if total_num - 10 <= 0:
                still_do = False
                per_send = total_num
            else:
                total_num -= 10
                per_send = 10
            div_req = req.copy()
            div_req[-3] = per_send

            # Obtain copies of various types of information
            tmp_resources_on_cluster = valueclone_nested_dict_proxy(
                manager_resources_on_cluster
            )
            tmp_tasks_execute_situation_on_each_node = valueclone_nested_dict_proxy(
                manager_task_dict
            )
            tmp_resources_on_each_node_dict = valueclone_nested_dict_proxy(
                manager_resources_on_each_node_dict
            )

            state = get_observation(
                div_req,
                manager_resources_on_cluster,
                tmp_tasks_execute_situation_on_each_node,
                manager_resources_on_each_node_dict,
            )
            state = np.array(state)

            if train_model:
                if 1000 > total_numsteps[0]:
                    action = my_action_space_sample.sample()  # Sample random action
                else:
                    if random.random() < 0.99:
                        action = agent.select_action(state)  # Sample action from policy
                        action = action[0]
                    else:
                        action = my_action_space_sample.sample()  # Sample random action
            else:
                action = agent.select_action(state)
                action = action[0]

            record_shape = action.shape
            flaten_action = action.flatten()

            action_result_list = (flaten_action * mask_np).tolist()
            max_pro = 0
            same_pro_list_index = []
            for i in range(len(action_result_list)):
                if action_result_list[i] > max_pro:
                    same_pro_list_index.clear()
                    same_pro_list_index.append(i)
                    max_pro = action_result_list[i]
                elif action_result_list[i] == max_pro:
                    same_pro_list_index.append(i)

            # Get the target worker node
            index_max_value = random.choice(same_pro_list_index)
            node_target = master_index_dict_rev[index_max_value]
            div_req[-1], div_req[4], div_req[2] = (
                node_target,
                GNN_kind + "-SAC",
                str(task_id),
            )
            master_ip = get_master_ip(div_req[-1])

            # Make centralized request assignments
            send_req_to_port(div_req, master_ip, BE_FROM_OHTER_PORT)

            back_np_action = np.array(action_result_list)
            action = np.reshape(back_np_action, record_shape)
            tmp_tasks_execute_situation_on_each_node = valueclone_nested_dict_proxy(
                manager_task_dict
            )
            next_state = get_observation(
                div_req,
                manager_resources_on_cluster,
                tmp_tasks_execute_situation_on_each_node,
                manager_resources_on_each_node_dict,
            )
            reward = sac_calcureward(
                tmp_tasks_execute_situation_on_each_node,
                node_target,
                manager_resources_on_each_node_dict,
            )
            memory.push(state, action, reward, next_state, 1)
            reward_record.append(reward)

            if train_model:
                if len(memory) > args.batch_size:
                    if total_numsteps[0] % 20 == 0:
                        # number of updates per step in environment
                        for i in range(args.updates_per_step):
                            # Update parameters of all the networks
                            with memory_lock:
                                (
                                    critic_1_loss,
                                    critic_2_loss,
                                    policy_loss,
                                    ent_loss,
                                    alpha,
                                ) = agent.update_parameters(
                                    memory, args.batch_size, updates
                                )
                            updates += 1
                total_numsteps[0] += 1

                if len(reward_record) % 8 == 0 and len(reward_record) != 0:
                    avg_reward_record = sum(reward_record) / len(reward_record)
                    reward_record.clear()
                    if record_sum < record_sum + avg_reward_record:
                        record_sum += avg_reward_record
                        agent.save_checkpoint("sac")
                    with open("./sac_reward.csv", "a+", newline="") as f:
                        csv_write = csv.writer(f)
                        csv_write.writerow([0, avg_reward_record])


def sac_calcureward(
    tmp_tasks_execute_situation_on_each_node,
    my_choice_node,
    tmp_resources_on_each_node_dict,
    check=False,
):
    node_resNeed_list = []
    node_resNeed_dict = {}
    same_as_me_dict = {}
    my_choice_resNeed = None

    for master in tmp_tasks_execute_situation_on_each_node:
        for node in tmp_tasks_execute_situation_on_each_node[master]:
            res_cpu_need_sum = 0
            for service_id in tmp_tasks_execute_situation_on_each_node[master][node]:
                if service_id not in LC_SERVICE_LIST:
                    res_cpu_need_sum += (
                        tmp_tasks_execute_situation_on_each_node[master][node][
                            service_id
                        ]["ST"]
                        * BE_CPU_ONCE_NEED_DICT[service_id]
                    )
            node_resNeed_list.append(res_cpu_need_sum)
            node_resNeed_dict[node] = res_cpu_need_sum

            if node == my_choice_node:
                my_choice_resNeed = res_cpu_need_sum

    for node in node_resNeed_dict:
        if node_resNeed_dict[node] == my_choice_resNeed:
            map_master = None
            REAL_BE_CPU = 0
            for master in EDGEMASTER_IP_DICTSET:
                if node in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
                    map_master = master
                    break
            for service_id in tmp_resources_on_each_node_dict[map_master][node]:
                if str(service_id) not in LC_SERVICE_LIST:
                    REAL_BE_CPU += tmp_resources_on_each_node_dict[map_master][node][
                        service_id
                    ]["cpu_use"]
            same_as_me_dict[node] = REAL_BE_CPU

    if my_choice_resNeed == 0:
        reward_base = 5
        if same_as_me_dict[my_choice_node] == 0:
            reward = reward_base + 5
        else:
            order_list_tuple = sorted(
                same_as_me_dict.items(), key=lambda s: s[1], reverse=True
            )
            order_node_name = [j for i in order_list_tuple for j in i if type(j) == str]
            rank_num = (
                len(node_resNeed_dict)
                - len(order_node_name)
                + order_node_name.index(my_choice_node)
                + 1
            )
            reward = (2 * rank_num - 9) / 7 * 10 + reward_base
    else:
        node_resNeed_list.sort(reverse=True)
        rank_num = 1 + node_resNeed_list.index(my_choice_resNeed)
        reward = (2 * rank_num - 9) / 7 * 10

    return reward


def classificate_request_from_edge(
    cache_queue,
    manager_resources_on_cluster,
    collect_tasks_execute_situation_on_each_node,
    manager_resources_on_each_node_dict,
    edge_queue_dict,
):
    while True:
        req = cache_queue.get()
        pickle.dumps(req)


def async_BE_threadExecute(
    req,
    manager_resources_on_cluster,
    collect_tasks_execute_situation_on_each_node,
    manager_resources_on_each_node_dict,
    myCPU_lock_dict,
    tmp_write_lock,
):

    cluster = random.choice([i for i in EDGEMASTER_IP_DICTSET.keys()])
    cluster_ip = EDGEMASTER_IP_DICTSET[cluster]["IP"]
    target_node = random.choice([i for i in EDGEMASTER_IP_DICTSET[cluster]["nodeset"]])
    req[-1] = target_node

    back_req = pickle.dumps(req)
    client_to = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_to.connect((cluster_ip, CLOUD_NODE_BE_DECISION_PORT))
    client_to.sendall(bytes(back_req))
    client_to.close()


def send_task_json(client, result):
    result = json.dumps(result)
    client.sendall(bytes(result.encode("utf-8")))


def run_once(command):
    return os.popen(command).read()


def run(command, keyword):
    result = run_once(command)
    if keyword is not None:
        initial_time = 0
        while result.find(keyword) == -1 and initial_time < 23:
            result = run_once(command)
            initial_time += 1
    return result


def sendtoMyself():
    mission = {"success": 55, "failure": 55, "stuck": 55}
    detail_mission = {
        "name": "test1",
        "service_type": "test1",
        "success": 55,
        "failure": 55,
    }

    client1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client1.connect((CENTRAL_MASTER_IP, CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT))

    send_task_json(client1, mission)
    # send_task_json(client2, detail_mission)

    client1.close()


def valueclone_nested_dict_proxy(dict_proxy):
    from multiprocessing.managers import BaseProxy

    dict_copy = dict_proxy._getvalue()
    for key, value in dict_copy.items():
        if isinstance(value, BaseProxy):
            dict_copy[key] = valueclone_nested_dict_proxy(value)
    return dict_copy


def clear_all(this_master_name, *dict_proxies):
    for dict_proxy in dict_proxies:
        if this_master_name in dict_proxy:
            dict_proxy[this_master_name].clear()


def THR_S_update(LC_S, S_LOCK, s):
    while True:
        c, addr = s.accept()
        node_service_updatenNum = c.recv(1024)
        node_service_updatenNum = json.loads(node_service_updatenNum.decode("utf-8"))
        with S_LOCK:
            if node_service_updatenNum[2] < 0:
                LC_S.remove((node_service_updatenNum[0], node_service_updatenNum[1]))
            elif node_service_updatenNum[2] > 0:
                LC_S.add((node_service_updatenNum[0], node_service_updatenNum[1]))
        c.close()


def lc_schedule_orchetra():

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((CENTRAL_MASTER_IP, CLOUD_LC_SET_UPDATE_PORT))
    s.listen(100)
    LC_S = set()
    S_LOCK = threading.Lock()

    S_update_thr = threading.Thread(target=THR_S_update, args=(LC_S, S_LOCK, s))
    S_update_thr.start()
    S_update_thr.join()


def THR_collect_data(mission, manager, mdata_lock, managerState):
    mission = json.loads(mission.decode("utf-8"))
    if "BE" in mission:
        with mdata_lock:
            tmp1_dict = managerState["BE"]
            tmp1_dict["success"] += mission["BE"]["success"]
            tmp1_dict["failure"] += mission["BE"]["failure"]
            tmp1_dict["stuck"] += mission["BE"]["stuck"]
            # managerState["BE"] = tmp1_dict
    else:
        with mdata_lock:
            tmp2_dict = managerState["LC"]
            tmp2_dict["success"] += mission["LC"]["success"]
            tmp2_dict["failure"] += mission["LC"]["failure"]
            tmp2_dict["stuck"] += mission["LC"]["stuck"]


def collect_data_excute(collect_data_queue, mdata_lock, managerState, manager):
    while True:
        mission = collect_data_queue.get()
        thr = threading.Thread(
            target=THR_collect_data, args=(mission, manager, mdata_lock, managerState)
        )
        thr.start()


def collect_data(collect_data_queue):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((CENTRAL_MASTER_IP, CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT))
    s.listen(1000)

    while True:
        c, addr = s.accept()
        mission = c.recv(2048 * 5)
        collect_data_queue.put(mission)
        c.close()


def THR_collect_tasks_execute_situation_on_each_node(pool_args):
    mission, task_lock_dict, manager_task_dict = pool_args
    mission = json.loads(
        mission.decode("utf-8")
    )  # masterName,service_type,successNum,failureNum
    # masterName, service_type, success, stuck, failure = mission['name'], mission['service_type'], mission['success'], mission['stuck'], mission['failure']
    masterName, nodeName, service_id, success, stuck, failure = (
        mission["masterName"],
        mission["nodeName"],
        str(mission["service_id"]),
        int(mission["success"]),
        int(mission["stuck"]),
        int(mission["failure"]),
    )

    if success != 0:
        if stuck != 0:
            with task_lock_dict[nodeName][service_id]["SU"], task_lock_dict[nodeName][
                service_id
            ]["ST"]:
                manager_task_dict[masterName][nodeName][service_id]["SU"] += success
                manager_task_dict[masterName][nodeName][service_id]["ST"] += stuck
        else:
            with task_lock_dict[nodeName][service_id]["SU"]:
                manager_task_dict[masterName][nodeName][service_id]["SU"] += success
    elif failure != 0:
        if stuck != 0:
            with task_lock_dict[nodeName][service_id]["F"], task_lock_dict[nodeName][
                service_id
            ]["ST"]:
                manager_task_dict[masterName][nodeName][service_id]["F"] += failure
                manager_task_dict[masterName][nodeName][service_id]["ST"] += stuck
        else:
            with task_lock_dict[nodeName][service_id]["F"]:
                manager_task_dict[masterName][nodeName][service_id]["F"] += failure
    elif stuck != 0:
        with task_lock_dict[nodeName][service_id]["ST"]:
            manager_task_dict[masterName][nodeName][service_id]["ST"] += stuck


def collect_tasks_execute_situation_on_each_node_excute(
    collect_tasks_execute_situation_queue, task_lock_dict, manager_task_dict, manager
):
    pool = ThreadPoolExecutor()
    while True:
        mission = collect_tasks_execute_situation_queue.get()
        task = pool.submit(
            THR_collect_tasks_execute_situation_on_each_node,
            (mission, task_lock_dict, manager_task_dict),
        )
        task.add_done_callback(thread_pool_callback)


def collect_tasks_execute_situation_on_each_node(collect_tasks_execute_situation_queue):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((CENTRAL_MASTER_IP, TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT))
    server.listen(500)
    while True:
        c, addr = server.accept()
        mission = c.recv(1024)
        collect_tasks_execute_situation_queue.put(mission)
        c.close()


def THR_resources_on_each_node_excute(pool_args):
    resources, share_lock4, manager_resources_on_each_node_dict = pool_args
    try:
        resources = json.loads(resources.decode("utf-8"))
        with share_lock4:
            for master in resources:
                clear_all(master, manager_resources_on_each_node_dict)
                manager_resources_on_each_node_dict[master] = resources[master]
    except Exception as e:
        print("THR_resources_on_each_node_excute with error:", e)
    print(
        "listening:", valueclone_nested_dict_proxy(manager_resources_on_each_node_dict)
    )


def resources_on_each_node_excute(
    resources_on_each_node_queue, manager_resources_on_each_node_dict, share_lock4
):
    pool = ThreadPoolExecutor()

    while True:
        resources = resources_on_each_node_queue.get()
        # thr = threading.Thread(target=THR_resources_on_each_node_excute, args=(resources, share_lock4, manager_resources_on_each_node_dict))
        # thr.start()

        task = pool.submit(
            THR_resources_on_each_node_excute,
            (resources, share_lock4, manager_resources_on_each_node_dict),
        )
        task.add_done_callback(thread_pool_callback)


def resources_on_each_node(resources_on_each_node_queue):
    SEND_BUF_SIZE = 4096 * 10
    RECV_BUF_SIZE = 4096 * 10
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUF_SIZE)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECV_BUF_SIZE)
    server.bind((CENTRAL_MASTER_IP, RESOURCE_ON_EACH_NODE_PORT))
    server.listen(500)

    while True:
        c, addr = server.accept()
        resources = c.recv(6)
        length = int.from_bytes(resources, byteorder="big")
        # print("length", length)
        if length == 0:
            continue
        content = bytes()
        count = 0
        while True:
            value = c.recv(length)
            content = content + value
            count += len(value)
            if count >= length:
                break
        resources_on_each_node_queue.put(content)
        c.close()


def THR_collect_cluster_info_excute(pool_args):
    resources, share_lock5, manager_resources_on_cluster = pool_args
    resources = json.loads(resources.decode("utf-8"))
    with share_lock5:
        for master_name in resources:
            manager_resources_on_cluster[master_name] = resources[master_name]
    logger.info(str(valueclone_nested_dict_proxy(manager_resources_on_cluster)))


def collect_cluster_info_excute(
    collect_cluster_info_queue, manager_resources_on_cluster, share_lock5
):
    pool = ThreadPoolExecutor()

    while True:
        resources = collect_cluster_info_queue.get()
        task = pool.submit(
            THR_collect_cluster_info_excute,
            (resources, share_lock5, manager_resources_on_cluster),
        )
        task.add_done_callback(thread_pool_callback)


def collect_cluster_info(collect_cluster_info_queue):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((CENTRAL_MASTER_IP, COLLECT_CLUSTER_INFO))
    server.listen(500)

    while True:
        c, addr = server.accept()
        resources = c.recv(6)
        length = int.from_bytes(resources, byteorder="big")
        if length == 0:
            continue
        content = bytes()
        count = 0
        while True:
            value = c.recv(length)
            content = content + value
            count += len(value)
            if count >= length:
                break
        collect_cluster_info_queue.put(content)
        c.close()


def req_center_load_get(req_load_dict):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((CENTRAL_MASTER_IP, CLOUD_REQ_RES_UPDATE_PORT))
    server.listen(500)
    while True:
        s1, addr = server.accept()
        length_data = s1.recv(1024)
        if length_data:
            this_request = bytes() + length_data
            center_load_dict = pickle.loads(this_request)
            req_load_dict["LC"] = center_load_dict["LC"]
            req_load_dict["BE"] = center_load_dict["BE"]
            s1.close()


def count_task_num(task_dict):
    count_num_lc_su = 0
    count_num_lc_f = 0
    count_num_lc_st = 0

    count_num_be_su = 0
    count_num_be_f = 0
    count_num_be_st = 0

    for master in task_dict:
        for node in task_dict[master]:
            for server_id in task_dict[master][node]:
                if server_id in LC_SERVICE_LIST:
                    count_num_lc_su += task_dict[master][node][server_id]["SU"]
                    count_num_lc_f += task_dict[master][node][server_id]["F"]
                    count_num_lc_st += task_dict[master][node][server_id]["ST"]
                else:
                    count_num_be_su += task_dict[master][node][server_id]["SU"]
                    count_num_be_f += task_dict[master][node][server_id]["F"]
                    count_num_be_st += task_dict[master][node][server_id]["ST"]
    return (
        count_num_lc_su,
        count_num_lc_f,
        count_num_be_su,
        count_num_be_f,
        count_num_lc_st,
        count_num_be_st,
    )


def csv_collect_everthing(
    manager_resources_on_each_node_dict,
    manager_resources_on_cluster,
    manager_task_dict,
    req_load_dict,
):

    time_now = 0
    total_node_num = 0
    counters = 0
    record_lc_success = 0
    record_lc_failure = 0
    record_be_success = 0
    record_be_failure = 0

    for master in EDGEMASTER_IP_DICTSET:
        total_node_num += len(list(EDGEMASTER_IP_DICTSET[master]["nodeset"]))
    while True:
        sum_lc_cpu_use = 0
        sum_be_cpu_use = 0
        sum_lc_cpu_sh = 0
        sum_be_cpu_sh = 0
        sum_lc_meo_sh = 1000 * 3 * total_node_num
        sum_be_meo_sh = 1000 * 3 * total_node_num
        sum_be_meo_use = 1000 * 3 * total_node_num
        sum_lc_meo_use = 1000 * 3 * total_node_num

        task_dict = valueclone_nested_dict_proxy(manager_task_dict)
        docker_stats_dict = valueclone_nested_dict_proxy(
            manager_resources_on_each_node_dict
        )
        manager_resources_on_cluster_copy = valueclone_nested_dict_proxy(
            manager_resources_on_cluster
        )
        record_load = valueclone_nested_dict_proxy(req_load_dict)

        for master in docker_stats_dict:
            for node in docker_stats_dict[master]:
                for service_id in docker_stats_dict[master][node]:
                    if str(service_id) in LC_SERVICE_LIST:
                        sum_lc_cpu_use += docker_stats_dict[master][node][service_id][
                            "cpu_use"
                        ]
                        sum_lc_meo_use += docker_stats_dict[master][node][service_id][
                            "meo_use"
                        ]
                    else:
                        sum_be_cpu_use += docker_stats_dict[master][node][service_id][
                            "cpu_use"
                        ]
                        sum_be_meo_use += docker_stats_dict[master][node][service_id][
                            "meo_use"
                        ]

        for master in manager_resources_on_cluster_copy:
            for node in manager_resources_on_cluster_copy[master]:
                sum_lc_cpu_sh += manager_resources_on_cluster_copy[master][node]["cpu"][
                    "LC_CPU_SH"
                ]
                sum_be_cpu_sh += manager_resources_on_cluster_copy[master][node]["cpu"][
                    "BE_CPU_SH"
                ]
                sum_lc_meo_sh += manager_resources_on_cluster_copy[master][node]["meo"][
                    "LC_MEO"
                ]
                sum_be_meo_sh += manager_resources_on_cluster_copy[master][node]["meo"][
                    "BE_MEO"
                ]

        if counters % 50 == 0 and counters != 0:
            (
                record_lc_success,
                record_lc_failure,
                record_be_success,
                record_be_failure,
                _,
                _,
            ) = count_task_num(task_dict)
            counters += 1
            continue

        (
            lc_success_all,
            lc_failure_all,
            be_success_all,
            be_failure_all,
            _,
            _,
        ) = count_task_num(task_dict)

        lc_success = lc_success_all - record_lc_success
        lc_failure = lc_failure_all - record_lc_failure

        be_success = be_success_all - record_be_success
        be_failure = be_failure_all - record_be_failure

        if lc_success + lc_failure > 0:
            lc_success_rate = round(lc_success / (lc_success + lc_failure), 3)
            # print("lc_success_rate:",lc_success_rate)
        else:
            lc_success_rate = 0

        if be_success + be_failure > 0:
            be_success_rate = round(be_success / (be_success + be_failure), 3)

        else:
            be_success_rate = 0

        if record_load["LC"] >= 0:

            lc_cpu_use_rate = round(
                sum_lc_cpu_use / (total_node_num * CPU_TOTAL_INIT), 5
            )
            be_cpu_use_rate = round(
                sum_be_cpu_use / (total_node_num * CPU_TOTAL_INIT), 5
            )

            lc_cpu_sh_rate = round(sum_lc_cpu_sh / (total_node_num * CPU_TOTAL_INIT), 5)
            be_cpu_sh_rate = round(sum_be_cpu_sh / (total_node_num * CPU_TOTAL_INIT), 5)

            lc_meo_sh_rate = round(sum_lc_meo_sh / (total_node_num * MEO_TOTAL_INIT), 5)
            be_meo_sh_rate = round(sum_be_meo_sh / (total_node_num * MEO_TOTAL_INIT), 5)

            lc_meo_use_rate = round(
                sum_lc_meo_use / (total_node_num * MEO_TOTAL_INIT), 5
            )
            be_meo_use_rate = round(
                sum_be_meo_use / (total_node_num * MEO_TOTAL_INIT), 5
            )

            req_center_res_rate = round(sum(list(record_load.values())) / 100, 2)
            record_list = [
                lc_cpu_use_rate,
                be_cpu_use_rate,
                lc_cpu_sh_rate,
                be_cpu_sh_rate,
                lc_meo_use_rate,
                be_meo_use_rate,
                lc_success_rate,
                be_success_rate,
                req_center_res_rate,
                lc_meo_sh_rate,
                be_meo_sh_rate,
                time_now,
            ]

            with open("./collect_everything.csv", "a+", newline="") as f:
                csv_write = csv.writer(f)
                csv_write.writerow(record_list)

            time_now += 0.1
            counters += 1
        time.sleep(0.1)


def collect_tasks_time_get(info_queue):

    cloud_receive_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cloud_receive_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    cloud_receive_server.bind(("0.0.0.0", CLOUD_RESULT_PORT))
    cloud_receive_server.listen(1000)
    while True:
        s1, addr = cloud_receive_server.accept()
        req_result = s1.recv(1024 * 2)
        req_result = json.loads(req_result.decode("utf-8"))
        info_queue.put(req_result)
        s1.close()


def thr_collect_tasks_time_write(pool_args):
    req_result, tmp_write_lock = pool_args
    with tmp_write_lock:
        with open("./collect_req.csv", "a+", newline="") as f:
            csv_write = csv.writer(f)
            csv_write.writerow(req_result)


def collect_tasks_time_write(info_queue, tmp_write_lock):
    pool = ThreadPoolExecutor()
    relative_time = time.time()

    while True:
        req_result = info_queue.get()  # req, use_time
        req_result.append(time.time() - relative_time)
        # thr = threading.Thread(target=thr_collect_tasks_time_write, args=(req_result, tmp_write_lock))
        # thr.start()

        task = pool.submit(thr_collect_tasks_time_write, (req_result, tmp_write_lock))
        task.add_done_callback(thread_pool_callback)


def make_pod(pod_dict):
    pod = Pod("", "", "", "", "", "", "", "")
    pod.__dict__ = pod_dict
    return pod


def send_req_to_port(req, target_ip, port):
    req = pickle.dumps(req)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((target_ip, port))
    client.sendall(bytes(req))
    client.close()


def get_master_ip(nodeName):
    for master in EDGEMASTER_IP_DICTSET:
        if nodeName in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
            master_ip = EDGEMASTER_IP_DICTSET[master]["IP"]
            return master_ip
