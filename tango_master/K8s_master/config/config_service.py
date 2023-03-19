"""
    Specific parameter settings and criteria for services and requests
"""

# example: The ID corresponding to the LC service request
LC_SERVICE_LIST = ['1', ]
LC_CIRCLETIMES_DICT = {'1': 1, }
BE_SERVICE_LIST = ['11', ]
ALL_SERVICE_LIST = LC_SERVICE_LIST + BE_SERVICE_LIST

# example: CPU, Meo requirements from pressure testing
LC_CPU_ONCE_NEED_DICT = {'1': [1200, 0], }
LC_MEO_ONCE_NEED_DICT = {'1': 34, }
CONTAINER_INIT_MEO = {'1': 100, "0": 100}
BE_CPU_ONCE_NEED_DICT = {'11': 1200, }

# example: Parameter setting for service request load
LC_LOAD_DICT = {'1': {"cpu": "300", "mem": "0"}, }
BE_LOAD_DICT = {'11': {"cpu": "650", "mem": "43"}, }

# example: Standard delay time of LC determined by piezometry
ALL_SERVICE_EXECUTE_STANDARDTIME = {'1': 100, }  # ms

for each_serviceID in LC_MEO_ONCE_NEED_DICT:
    LC_LOAD_DICT[each_serviceID]["mem"] = str(LC_MEO_ONCE_NEED_DICT[each_serviceID])
