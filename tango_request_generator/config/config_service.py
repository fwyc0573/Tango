"""
    Specific parameter settings and criteria for services and requests
"""

LC_SERVIVE_DICT = [
    "1",
]

BE_SERVIVE_DICT = [
    "11",
]


class ReqData(object):
    def __init__(
        self,
        service_type,
        start_time,
        qos_res_change=0,
        first_tip="",
        policy="",
        lc_load=0,
        be_load=0,
        req_delay=0,
        req_num=0,
        task_class="",
        target_node="",
    ):
        self.service_type = int(service_type)
        self.start_time = float(start_time)
        self.qos_res_change = qos_res_change
        self.first_tip = ""
        self.policy = ""
        self.lc_load = lc_load
        self.be_load = be_load
        self.req_delay = req_delay
        self.req_num = req_num
        self.task_class = task_class
        self.target_node = target_node

    def __repr__(self):
        return f"ReqData(service_type={self.service_type},start_time={self.start_time},\
        ,qos_res_change={self.qos_res_change}, first_tip={self.first_tip}, policy={self.policy}, lc_load={self.lc_load}, be_load={self.be_load}, \
        req_delay={self.req_delay}, req_num={self.req_num}, task_class={self.task_class},target_node={self.target_node})"
