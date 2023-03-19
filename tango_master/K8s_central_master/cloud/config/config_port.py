"""
    Relevant port serial numbers involved
"""

# The port of the edge cluster master collecting tasks from the task sender
EDGE_MASTER_RECEIVE_REQUEST_PORT = 9000
# BE receive port
DISPATCH_RECEIVE_REQUEST_PORT = 9000
# Collect the status of tasks execution
CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT = 9001
# Send to the cloud to decide
CLOUD_MASTER_RECEIVE_REQUEST_PORT = 9002
# Request an update
CLOUD_MASTER_RECEIVE_UPDATE_PORT = 9003
# Number of successful and failed requests of each type in each node since the last scheduling
TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT = 9004
# Current service deployment types and numbers for each node
CURRENT_SERVICE_ON_EACH_NODE_PORT = 9005
# Current service deployment types and numbers for each node
# STUCK_TASKS_SITUATION_ON_EACH_NODE_PORT = 9006
# Memory usage by node
RESOURCE_ON_EACH_NODE_PORT = 9007
# Edge receives updated results
EDGE_MASTER_RECEIVE_UPDATE = 9008
# Collect clusters info
COLLECT_CLUSTER_INFO = 9010
# Receive BE decisions from the center
CLOUD_NODE_BE_DECISION_PORT = 9013
# Pre-service scheduling decisions for receiving LCs from the center
CLOUD_NODE_LC_LAYOUT_PORT = 9014
# Receive pre-request assignment decisions from the center for LC
CLOUD_NODE_LC_DISTRIBUTE_PORT = 9015
# LC service orchestration SET update port
CLOUD_LC_SET_UPDATE_PORT = 8999
# Request central load update port
CLOUD_REQ_RES_UPDATE_PORT = 8996
# Central message receiving port
CLOUD_RESULT_PORT = 8993
# BE pending request interface
BE_FROM_OHTER_PORT = 8995
# BE reward interface
BE_REWARD_PORT = 8992