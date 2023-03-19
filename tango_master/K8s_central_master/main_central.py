import psutil
import multiprocessing
from multiprocessing import Manager, Queue
from cloud.central_master import collect_data
# from cloud.cloud import execute
from cloud.central_master import lc_schedule_orchetra
from cloud.central_master import collect_tasks_execute_situation_on_each_node
# from cloud.cloud import current_service_on_each_node
# from cloud.cloud import stuck_tasks_situation_on_each_node
from cloud.central_master import resources_on_each_node
from cloud.central_master import valueclone_nested_dict_proxy
from cloud.central_master import collect_cluster_info
from cloud.central_master import receive_request_from_edge
from cloud.central_master import classificate_request_from_edge
from cloud.central_master import centralized_scheduling_dispatcher
from cloud.central_master import EDGEMASTER_IP_DICTSET
from cloud.central_master import csv_collect_everthing
from cloud.central_master import collect_tasks_time_get
from cloud.central_master import collect_tasks_time_write
from cloud.central_master import collect_tasks_execute_situation_on_each_node_excute
from cloud.central_master import collect_data_excute
from cloud.central_master import collect_cluster_info_excute
from cloud.central_master import resources_on_each_node_excute
from cloud.central_master import req_center_load_get
from cloud.central_master import task_reward_get
from cloud.central_master import task_dict_update
from cloud.central_master import ALL_SERVICE_LIST


if __name__ == '__main__':

    share_lock4 = multiprocessing.Lock()
    share_lock5 = multiprocessing.Lock()
    tmp_write_lock = multiprocessing.Lock()
    task_lock_dict = {}

    with Manager() as manager:

        manager_resources_on_each_node_dict = manager.dict()  # Information dict about containers
        manager_resources_on_cluster = manager.dict()  # Information dict of edge clusters
        mdata = manager.dict()
        tmp1_dict = manager.dict({'success': 0, 'failure': 0, 'stuck': 0})
        mdata["BE"] = tmp1_dict
        tmp2_dict = manager.dict({'success': 0, 'failure': 0, 'stuck': 0})
        mdata["LC"] = tmp2_dict

        manager_task_dict = manager.dict()  # Detailed dictionary of requests
        for master in EDGEMASTER_IP_DICTSET:
            manager_task_dict[master] = manager.dict()
            for node in EDGEMASTER_IP_DICTSET[master]["nodeset"]:
                manager_task_dict[master][node] = manager.dict()
                task_lock_dict[node] = {}
                for service_id in ALL_SERVICE_LIST:
                    manager_task_dict[master][node][service_id] = manager.dict({'SU': 0, 'F': 0, 'ST': 0})
                    task_lock_dict[node][service_id] = {}
                    task_lock_dict[node][service_id]["SU"] = multiprocessing.Lock()
                    task_lock_dict[node][service_id]["F"] = multiprocessing.Lock()
                    task_lock_dict[node][service_id]["ST"] = multiprocessing.Lock()

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Request processing for all clusters
        """
        collect_tasks_execute_situation_queue = Queue(maxsize=500)
        p4 = multiprocessing.Process(target=collect_tasks_execute_situation_on_each_node,
                                     args=(collect_tasks_execute_situation_queue,))
        p4.start()
        P = psutil.Process(p4.pid)
        P.cpu_affinity([1, 2])
        for i in range(3):
            p4 = multiprocessing.Process(target=collect_tasks_execute_situation_on_each_node_excute,
                                         args=(collect_tasks_execute_situation_queue, task_lock_dict, manager_task_dict,
                                               manager))
            p4.start()
            P = psutil.Process(p4.pid)
            P.cpu_affinity([3, 8])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        # p5 = multiprocessing.Process(target=current_service_on_each_node,
        # args=(manager_current_service_on_each_node_dict, share_lock2))
        # p5.start()
        # for master in EDGEMASTER_IP_DICTSET:
        # manager_resources_on_each_node_dict[master] = manager.dict()

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Container Information
        """
        resources_on_each_node_queue = Queue(maxsize=500)
        p7 = multiprocessing.Process(target=resources_on_each_node, args=(resources_on_each_node_queue,))
        p7.start()
        P = psutil.Process(p7.pid)
        P.cpu_affinity([1, 2])
        for i in range(3):
            p7 = multiprocessing.Process(target=resources_on_each_node_excute, args=(
            resources_on_each_node_queue, manager_resources_on_each_node_dict, share_lock4))
            p7.start()
            P = psutil.Process(p7.pid)
            P.cpu_affinity([3, 8])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Edge Cluster Information
        """
        collect_cluster_info_queue = Queue(maxsize=500)
        p8 = multiprocessing.Process(target=collect_cluster_info, args=(collect_cluster_info_queue,))
        p8.start()
        P = psutil.Process(p8.pid)
        P.cpu_affinity([1, 2])
        for i in range(3):
            p8 = multiprocessing.Process(target=collect_cluster_info_excute,
                                         args=(collect_cluster_info_queue, manager_resources_on_cluster, share_lock5))
            p8.start()
            P = psutil.Process(p8.pid)
            P.cpu_affinity([3, 8])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        # Temporary total TASK information for proofreading
        # collect_data_queue = Queue(maxsize = 3000)
        # p3 = multiprocessing.Process(target=collect_data, args=(collect_data_queue,))
        # p3.start()

        # for i in range(2):
        # p3 = multiprocessing.Process(target=collect_data_excute, args=(collect_data_queue, mdata_lock, mdata, manager))
        # p3.start()

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            BE Centralized Scheduling
        """
        cache_queue = Queue(maxsize=1000)  # Undifferentiated BE request reception
        edge_queue_dict = {}  # BE Request Queue by Cluster Category
        for master in EDGEMASTER_IP_DICTSET:
            edge_queue_dict[master] = Queue(maxsize=1000)

        # task_info_queue = Queue(maxsize = 500)
        reward_back_queue = Queue(maxsize=1000)
        # task_info_dict = manager.dict()
        tmp_lock = multiprocessing.Lock()

        # Receiving Requests
        process_receive_request_from_edge = multiprocessing.Process(target=receive_request_from_edge,
                                                                    args=(cache_queue,))

        # Classification Request
        # process_classificate_request_from_edge = multiprocessing.Process(target=classificate_request_from_edge, \
        # args=(cache_queue, manager_resources_on_cluster,collect_tasks_execute_situation_on_each_node, manager_resources_on_each_node_dict, edge_queue_dict))

        process_receive_request_from_edge.start()
        P = psutil.Process(process_receive_request_from_edge.pid)
        P.cpu_affinity([1, 2])

        # process_classificate_request_from_edge.start()
        # P = psutil.Process(process_classificate_request_from_edge.pid)
        # P.cpu_affinity([3,8])

        # Processing Requests
        for i in range(1):
            process_centralized_scheduling_dispatcher = multiprocessing.Process(target=centralized_scheduling_dispatcher,
                                                                       args=(cache_queue, edge_queue_dict,
                                                                             manager_resources_on_cluster,
                                                                             manager_task_dict,
                                                                             manager_resources_on_each_node_dict, \
                                                                             reward_back_queue, tmp_lock))
            process_centralized_scheduling_dispatcher.start()
            P = psutil.Process(process_centralized_scheduling_dispatcher.pid)
            P.cpu_affinity([3, 8])

        process_reward_get = multiprocessing.Process(target=task_reward_get,
                                                     args=(reward_back_queue,))
        process_reward_get.start()
        P = psutil.Process(process_reward_get.pid)
        P.cpu_affinity([1, 2])

        # process_task_dict_update = multiprocessing.Process(target=task_dict_update,args=(task_info_queue, task_info_dict, manager))  # task_info_update
        # process_task_dict_update.start()
        # P = psutil.Process(process_task_dict_update.pid)
        # P.cpu_affinity([3,8])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Service Orchestration
        """
        p2 = multiprocessing.Process(target=lc_schedule_orchetra)
        p2.start()

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Load get
        """
        req_load_dict = manager.dict({"LC": -1, "BE": -1})
        p3 = multiprocessing.Process(target=req_center_load_get, args=(req_load_dict,))
        p3.start()
        P = psutil.Process(p3.pid)
        P.cpu_affinity([1, 2])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Data logging
        """
        p0 = multiprocessing.Process(target=csv_collect_everthing, args=(
        manager_resources_on_each_node_dict, manager_resources_on_cluster, manager_task_dict, req_load_dict))
        p0.start()
        P = psutil.Process(p0.pid)
        P.cpu_affinity([3, 8])
        # print("P.nice():", P.nice())
        # P.nice(-10)

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        """
            Individual request completion
        """
        info_queue = Queue(maxsize=500)
        p6 = multiprocessing.Process(target=collect_tasks_time_get, args=(info_queue,))
        p6.start()
        P = psutil.Process(p6.pid)
        P.cpu_affinity([1, 2])
        for i in range(1):
            p6 = multiprocessing.Process(target=collect_tasks_time_write,
                                         args=(info_queue, tmp_write_lock,))
            p6.start()
            P = psutil.Process(p6.pid)
            P.cpu_affinity([3, 8])

        # ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        p6.join()
        p0.join()
        p8.join()