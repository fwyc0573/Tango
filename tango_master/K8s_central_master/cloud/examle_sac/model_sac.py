import math
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.distributions import Normal
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv, SAGEConv, GATConv

GNN_kind = "graphsage"

LOG_SIG_MAX = 2
LOG_SIG_MIN = -20
epsilon = 1e-6

# Initialize Policy weights
def weights_init_(m):
    if isinstance(m, nn.Linear):
        torch.nn.init.xavier_uniform_(m.weight, gain=1)
        torch.nn.init.constant_(m.bias, 0)


class ValueNetwork(nn.Module):
    def __init__(self, num_inputs, hidden_dim):
        super(ValueNetwork, self).__init__()

        self.linear1 = nn.Linear(num_inputs, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, hidden_dim)
        self.linear3 = nn.Linear(hidden_dim, 1)

        self.apply(weights_init_)

    def forward(self, state):
        x = F.relu(self.linear1(state))
        x = F.relu(self.linear2(x))
        x = self.linear3(x)
        return x


class QNetwork(nn.Module):
    def __init__(self, num_inputs, num_actions, hidden_dim):
        super(QNetwork, self).__init__()

        print("num_inputs:", num_inputs, "num_actions:", num_actions)
        if GNN_kind == "graphsage":
            self.conv1 = SAGEConv(num_inputs, num_inputs)

        # Q1 architecture
        self.linear1 = nn.Linear(num_inputs + num_actions, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, hidden_dim)
        self.linear3 = nn.Linear(hidden_dim, 1)

        # Q2 architecture
        self.linear4 = nn.Linear(num_inputs + num_actions, hidden_dim)
        self.linear5 = nn.Linear(hidden_dim, hidden_dim)
        self.linear6 = nn.Linear(hidden_dim, 1)

        self.apply(weights_init_)

    def forward(self, data_list, action):
        all_tensor_list = None
        for each_data in data_list:

            out = F.relu(self.conv1(each_data.x, each_data.edge_index))
            x = out + each_data.x
            if all_tensor_list == None:
                all_tensor_list = x
            else:
                all_tensor_list = torch.cat((all_tensor_list, x))

        xu = torch.cat([all_tensor_list, action], 2)
        # xu = torch.cat([state, action], 1)

        x1 = F.relu(self.linear1(xu))
        x1 = F.relu(self.linear2(x1))
        x1 = self.linear3(x1)

        x2 = F.relu(self.linear4(xu))
        x2 = F.relu(self.linear5(x2))
        x2 = self.linear6(x2)

        return x1, x2


class GNNParser:
    """
    Parser converting raw environment observations to agent inputs (s_t).
    """

    def __init__(
        self,
        nregion,
        input_size,
        edge_index,
        T=10,
        grid_h=1,
        grid_w=1,
        scale_factor=0.01,
    ):
        super().__init__()
        self.nregion = nregion
        self.input_size = input_size
        self.T = T
        self.s = scale_factor
        self.grid_h = grid_h
        self.grid_w = grid_w
        self.edge_index = edge_index

    def parse_obs(self, obs):
        date_list = []
        # print("obs:", obs)
        for each_obs in obs:
            pre_x = []
            for node in each_obs:

                tmp_list = []
                for fea in node:
                    tmp_list.append(fea)
                pre_x.append(tmp_list)

            pre_x_mean = np.array(pre_x) - np.mean(np.array(pre_x), axis=0)
            pre_x = np.array([pre_x_mean])  # np.array() [each_obs]
            x = torch.tensor(pre_x)
            x = x.to(torch.float32)
            # x = x.squeeze(0).view(self.input_size, self.nregion).T
            # edge_index, pos_coord = grid(height=self.grid_h, width=self.grid_w)
            edge_index = torch.tensor(self.edge_index)
            data = Data(x, edge_index)

            date_list.append(data)

        return date_list


class GaussianPolicy(nn.Module):
    def __init__(self, num_inputs, num_actions, hidden_dim, action_space=None):
        super(GaussianPolicy, self).__init__()

        if GNN_kind == "graphsage":
            self.conv1 = SAGEConv(num_inputs, num_inputs)

        self.linear1 = nn.Linear(num_inputs, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, hidden_dim)

        self.mean_linear = nn.Linear(hidden_dim, num_actions)
        self.log_std_linear = nn.Linear(hidden_dim, num_actions)

        self.apply(weights_init_)

        # action rescaling
        if action_space is None:
            self.action_scale = torch.tensor(1.0)
            self.action_bias = torch.tensor(0.0)
        else:
            self.action_scale = torch.FloatTensor(
                (action_space.high - action_space.low) / 2.0
            )
            self.action_bias = torch.FloatTensor(
                (action_space.high + action_space.low) / 2.0
            )
            print(
                "action_scale:", self.action_scale, "\naction_bias:", self.action_bias
            )

    def forward(self, data_list):
        all_tensor_list = None
        for each_data in data_list:
            out = F.relu(self.conv1(each_data.x, each_data.edge_index))
            x = out + each_data.x
            if all_tensor_list == None:
                all_tensor_list = x
            else:
                all_tensor_list = torch.cat((all_tensor_list, x))
        # print("all_tensor_list:", all_tensor_list)
        # raise

        all_tensor_list = F.relu(self.linear1(all_tensor_list))
        all_tensor_list = F.relu(self.linear2(all_tensor_list))

        mean = self.mean_linear(all_tensor_list)
        log_std = self.log_std_linear(all_tensor_list)
        log_std = torch.clamp(log_std, min=LOG_SIG_MIN, max=LOG_SIG_MAX)

        return mean, log_std

    def sample(self, data_list):
        mean, log_std = self.forward(data_list)
        # print("mean:", mean)
        # print("log_std:", log_std)
        # raise

        std = log_std.exp()
        normal = Normal(mean, std)
        x_t = normal.rsample()  # for reparameterization trick (mean + std * N(0,1))
        y_t = torch.tanh(x_t)
        action = y_t * self.action_scale + self.action_bias
        log_prob = normal.log_prob(x_t)
        # Enforcing Action Bound
        log_prob -= torch.log(self.action_scale * (1 - y_t.pow(2)) + epsilon)
        log_prob = log_prob.sum(1, keepdim=True)
        mean = torch.tanh(mean) * self.action_scale + self.action_bias
        # print("action:", action)
        # print("mean:", mean)
        # print("log_prob:", log_prob)
        return action, log_prob, mean

    def to(self, device):
        self.action_scale = self.action_scale.to(device)
        self.action_bias = self.action_bias.to(device)
        return super(GaussianPolicy, self).to(device)


class DeterministicPolicy(nn.Module):
    def __init__(self, num_inputs, num_actions, hidden_dim, action_space=None):
        super(DeterministicPolicy, self).__init__()
        self.linear1 = nn.Linear(num_inputs, hidden_dim)
        self.linear2 = nn.Linear(hidden_dim, hidden_dim)

        self.mean = nn.Linear(hidden_dim, num_actions)
        self.noise = torch.Tensor(num_actions)

        self.apply(weights_init_)

        # action rescaling
        if action_space is None:
            self.action_scale = 1.0
            self.action_bias = 0.0
        else:
            self.action_scale = torch.FloatTensor(
                (action_space.high - action_space.low) / 2.0
            )
            self.action_bias = torch.FloatTensor(
                (action_space.high + action_space.low) / 2.0
            )

    def forward(self, state):
        x = F.relu(self.linear1(state))
        x = F.relu(self.linear2(x))
        mean = torch.tanh(self.mean(x)) * self.action_scale + self.action_bias
        return mean

    def sample(self, state):
        mean = self.forward(state)
        noise = self.noise.normal_(0.0, std=0.1)
        noise = noise.clamp(-0.25, 0.25)
        action = mean + noise
        return action, torch.tensor(0.0), mean

    def to(self, device):
        self.action_scale = self.action_scale.to(device)
        self.action_bias = self.action_bias.to(device)
        self.noise = self.noise.to(device)
        return super(DeterministicPolicy, self).to(device)
