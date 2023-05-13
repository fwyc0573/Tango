"""
    Control knob of some API
"""

IS_LOCAL_BE = True
cloud_each_lcbe_record = True
cloud_each_task_situation = False
RES_SITUATION_SEND = True

"""
    LC's QoS re-assurance mechanism control knob
"""
QOS_REASS_bool = True

"""
    D-VPA, Resource balancer API Switch knob (master part)
    Control knobs for D-VPA and resource regulations (where the resource regulations part controls the resources of the 
    scheduling part on the master, and the resource regulations control requires additional adjustment of the 
    K8s QoS-level when the container is running)
"""
DVPA_REGU_bool = True
