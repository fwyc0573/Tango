import asyncio
import csv
import json
import pickle
import time
from sys import float_info
import os
import multiprocessing
from config.config_port import *
from config.config_service import *
from config.config_others import *
loop = asyncio.get_event_loop()


def create_req(dataset_path):
    req_data_list = []
    with open(dataset_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            req_list = ReqData(row[0], row[1])
            req_list.req_num = int(row[2])
            req_list.lc_load = int(row[3])
            req_list.be_load = int(row[4])
            req_data_list.append(req_list)
    return req_data_list


async def send_request(req_data):
    # target_cluster = tmp_cluster[random.randrange(len(tmp_cluster))]
    cluster = random.choice([i for i in EDGEMASTER_IP_DICTSET.keys()])

    target_IP = EDGEMASTER_IP_DICTSET[cluster]["IP"]

    req_data.first_tip = cluster
    if req_data.service_type in LC_SERVIVE_DICT:
        req_data.task_class = "LC"
        req_port = LC_NODE_DECISION_PORT
        # req_data.target_node = random.choice([i for i in EDGEMASTER_IP_DICTSET[cluster]["nodeset"]])

    elif req_data.service_type in BE_SERVIVE_DICT:
        req_data.task_class = "BE"
        req_port = BE_NODE_DECISION_PORT

    # req_data.qos_res_change
    req_data.start_time = round(time.time(), 5)
    data = [req_data.service_type, req_data.start_time, req_data.qos_res_change, req_data.first_tip, req_data.policy, req_data.req_delay, req_data.req_num, req_data.task_class, req_data.target_node]

    reader, writer = await asyncio.open_connection(target_IP, req_port)
    data = pickle.dumps(data)
    writer.write(data)
    writer.close()

    reader, writer = await asyncio.open_connection(CENTRAL_MASTER_IP, CLOUD_REQ_RES_UPDATE_PORT)
    send_cloud_info = {"LC": req_data.lc_load, "BE": req_data.be_load}
    send_cloud_info = pickle.dumps(send_cloud_info)
    writer.write(send_cloud_info)
    writer.close()


async def main_coroutine():
    count = 0
    while count < 150000:
        req_data_list = create_req(request_path)
        if count == 0:
            reader, writer = await asyncio.open_connection(CENTRAL_MASTER_IP, CLOUD_REQ_RES_UPDATE_PORT)
            send_cloud_info = {"LC": 1, "BE": 1}
            send_cloud_info = pickle.dumps(send_cloud_info)
            writer.write(send_cloud_info)
            writer.close()

        time_offset = loop.time() - req_data_list[0].start_time
        now_index = 0
        req_data_len = len(req_data_list)

        while now_index < req_data_len:
            time_now = loop.time() - time_offset
            time_to_wait = req_data_list[now_index].start_time - time_now
            if time_to_wait > float_info.epsilon:

                await asyncio.sleep(time_to_wait)
                time_now = loop.time() - time_offset

            last_index = now_index
            max_time = time_now + float_info.epsilon
            while now_index < req_data_len:
                req_data = req_data_list[now_index] 
                if req_data.start_time > max_time:
                    break
                now_index += 1

            batch_to_send = req_data_list[last_index:now_index]

            await asyncio.gather(*map(send_request, batch_to_send))
            logger.info(f'progress: {now_index}/{req_data_len}')
        count += 1
        time.sleep(2)

    time.sleep(10)
    reader, writer = await asyncio.open_connection(CENTRAL_MASTER_IP, CLOUD_REQ_RES_UPDATE_PORT)
    send_cloud_info = {"LC":-1, "BE":-1}
    send_cloud_info = pickle.dumps(send_cloud_info)
    writer.write(send_cloud_info)
    writer.close()


def run_another_center():
    os.popen("python3 /home/request/random_req_sender.py")


if __name__ == '__main__':
    # thr = threading.Thread(target=LC_reqDistribute_get, args=(queue, ))
    time.sleep(10)
    multiprocessing.Process(target=run_another_center).start()
    loop.run_until_complete(main_coroutine())