import json
import socket
from config.config_others import *
CENTRAL_NODE_LC_LAYOUT_PORT = 9014

def send_or():
    # example: Send LC service deployment commands to 2 clusters
    all_layout_dict = {'master70': {'1':{'node71': 1, 'node72': 1, 'node73': 1, 'node74': 1}, },
                        'master80': {'1':{'node81': 1, 'node82': 1, 'node83': 1, 'node84': 1}, }}
    
    for edgeCluster in all_layout_dict:
        edge_ip = EDGEMASTER_IP_DICTSET[edgeCluster]['IP']
        send_dict = json.dumps(all_layout_dict[edgeCluster])
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        client.connect((edge_ip, CENTRAL_NODE_LC_LAYOUT_PORT))
        client.sendall(bytes(send_dict.encode('utf-8')))
        client.close()



if __name__ == "__main__":
    send_or()