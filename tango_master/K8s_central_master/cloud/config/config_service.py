"""
    Specific parameter settings and criteria for services and requests
"""

BE_LOAD_DICT = {
    "11": {"cpu": "650", "mem": "43"},
}
BE_CPU_ONCE_NEED_DICT = {
    "11": 1200,
}
LC_SERVICE_LIST = [
    "1",
]
BE_SERVICE_LIST = [
    "11",
]
ALL_SERVICE_LIST = LC_SERVICE_LIST + BE_SERVICE_LIST
