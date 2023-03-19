"""
    Relevant port serial numbers involved
"""

# Central cluster receiving BE task port
DISPATCH_RECEIVE_REQUEST_PORT = 9000
# LC port of the edge cluster master collecting tasks from task senders (pending decision queue)
LC_NODE_DECISION_PORT = 9000
# The port where the central cluster master collects the execution of tasks
CLOUD_MASTER_COLLECT_TASKS_SITUATION_PORT = 9001
# Send to the central cluster to decide
CLOUD_MASTER_RECEIVE_REQUEST_PORT = 9002
# Request an update
CLOUD_MASTER_RECEIVE_UPDATE = 9003
# Number of successful and failed requests of each type in each node since the last scheduling
TASKS_EXECUTE_SITUATION_ON_EACH_NODE_PORT = 9004
# Current service deployment types and numbers for each node
CURRENT_SERVICE_ON_EACH_NODE_PORT = 9005
# Memory and CPU usage by node
RESOURCE_ON_EACH_NODE_PORT = 9007
# Receive updated results
EDGE_MASTER_RECEIVE_UPDATE = 9008
# Edge side receives CPU information in real time
EDGE_NODE_CPU_UPDATE_PORT = 9009
# collect_cluster info
COLLECT_CLUSTER_INFO = 9010
# Local LC processing data information
COLLECT_LC_INFO = 9011
# LC decision sent to worker node
EDGE_NODE_LC_UPDATE_PORT = 9012
# BE port of the edge cluster master collecting tasks from task senders (pending decision queue)
BE_NODE_DECISION_PORT = 9013
#  Pre-service scheduling decisions for receiving LCs from a central cluster
CLOUD_NODE_LC_LAYOUT_PORT = 9014
# Send requests to node (to ensure the same pace as the resource scaling and the same effect as the Master execution)
EDGE_NODE_LC_WORK_PORT = 9015
# LC service orchestration SET update port
CLOUD_LC_SET_UPDATE_PORT = 8999
# Edge cluster information synchronization communication interface
EDGE_INFO_SYNERGY_UPDATE_PORT = 8998
# LOAD update interface
EDGE_NODE_LOAD_UPDATE_PORT = 8997
# LC Pending Request Interface
LC_FROM_OHTER_PORT = 8996
# BE pending request interface
BE_FROM_OHTER_PORT = 8995
# LC Results Interface
LC_RESULT_PORT = 8994
# Central cluster information receiving port
CLOUD_RESULT_PORT = 8993
#  BE Reward Interface
BE_REWARD_PORT = 8992