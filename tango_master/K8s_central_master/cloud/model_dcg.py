from torch import nn
import torch.nn.functional as F
from scipy.optimize import root, fsolve
from scipy.stats import poisson as scipy_poisson
from scipy.stats import bernoulli as scipy_bernoulli
from torch.distributions import Dirichlet
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv, SAGEConv, GATConv
from torch_geometric.nn import global_mean_pool, global_max_pool
from torch_geometric.utils import grid
GNN_kind = 'graphsage'


class GNNParser():
    def __init__(self, nregion, input_size, T=10, grid_h=1, grid_w=1, scale_factor=0.01):
        super().__init__()
        self.nregion = nregion
        self.input_size = input_size
        self.T = T
        self.s = scale_factor
        self.grid_h = grid_h
        self.grid_w = grid_w

    def parse_obs(self, obs):
        global final_list
        pre_x = []
        for node in obs:
            tmp_list = []
            for fea in node:
                tmp_list.append(fea)
            pre_x.append(tmp_list)

        pre_x_mean = np.array(pre_x) - np.mean(np.array(pre_x), axis=0)
        x = torch.tensor(pre_x_mean)
        x = x.to(torch.float32)

        edge_index = torch.tensor(final_list)
        data = Data(x, edge_index)
        return data


class GNNActor(nn.Module):
    """
    Actor \pi(a_t | s_t) parametrizing the concentration parameters of a Dirichlet Policy.
    """

    def __init__(self, input_size, hidden_size, output_size):
        super().__init__()

        if GNN_kind == 'graphsage':
            self.conv1 = SAGEConv(input_size, 256)

        self.lin1 = nn.Linear(256, 128)
        self.lin2 = nn.Linear(128, 32)
        self.lin3 = nn.Linear(32, output_size)

    def forward(self, data):
        out = F.relu(self.conv1(data.x, data.edge_index))
        x = out + data.x
        x = F.relu(self.lin1(x))
        x = F.relu(self.lin2(x))
        x = self.lin3(x)

        return x


class GNNCritic(nn.Module):
    """
    Critic parametrizing the value function estimator V(s_t).
    """

    def __init__(self, input_size, hidden_size, output_size):
        super().__init__()

        if GNN_kind == 'graphsage':
            self.conv1 = SAGEConv(input_size, 256)

        self.lin1 = nn.Linear(256, 128)
        self.lin2 = nn.Linear(128, 32)
        self.lin3 = nn.Linear(32, output_size)

    def forward(self, data):
        out = F.relu(self.conv1(data.x, data.edge_index))
        x = out + data.x
        x = torch.sum(x, dim=0)
        x = F.relu(self.lin1(x))
        x = F.relu(self.lin2(x))
        x = self.lin3(x)
        return x


class A2C(nn.Module):
    """
    Advantage Actor Critic algorithm for the AMoD control problem.
    """

    def __init__(self, nregion, input_size, output_size, eps=np.finfo(np.float32).eps.item(),
                 device=torch.device("cpu")):
        super(A2C, self).__init__()
        self.nregion = nregion
        self.eps = eps
        self.input_size = input_size
        self.output_size = output_size
        self.hidden_size = input_size
        self.device = device

        self.actor = GNNActor(self.input_size, self.hidden_size, self.output_size)
        self.critic = GNNCritic(self.input_size, self.hidden_size, self.output_size)
        self.obs_parser = GNNParser(nregion, input_size)

        self.optimizers = self.configure_optimizers()

        # action & reward buffer
        self.saved_actions = []
        self.rewards = []
        self.to(self.device)

    def forward(self, obs, jitter=jitter_setting):
        """
        forward of both actor and critic
        """
        # parse raw environment data in model format
        x = self.parse_obs(obs).to(self.device)

        # actor: computes concentration parameters of a Dirichlet distribution
        a_out = self.actor(x)
        concentration = F.softplus(a_out).reshape(-1) + jitter

        # critic: estimates V(s_t)
        value = self.critic(x)
        return concentration, value

    def parse_obs(self, obs):
        state = self.obs_parser.parse_obs(obs)
        return state

    def select_action(self, obs):
        global mask_tensor_add, mask_tensor_mul
        concentration, value = self.forward(obs)
        # print("concentration", concentration)
        m = Dirichlet(concentration)

        action = m.sample()

        action, log_prob_action, value = torch.mul(mask_tensor_mul, action), torch.mul(mask_tensor_mul,
                                                                                       m.log_prob(action)), torch.mul(
            mask_tensor_mul, value)
        # self.saved_actions.append(SavedAction(m.log_prob(action), value))
        # return list(action.cpu().numpy()), m.log_prob(action), value
        return list(action.cpu().numpy()), log_prob_action, value

    def training_step(self):
        R = 0
        saved_actions = self.saved_actions
        policy_losses = []  # list to save actor (policy) loss
        value_losses = []  # list to save critic (value) loss
        returns = []  # list to save the true values

        print("self.rewards:", self.rewards)
        # calculate the true value using rewards returned from the environment
        for r in self.rewards[::-1]:
            # calculate the discounted value
            R = r + args.gamma * R
            returns.insert(0, R)
        # print('self.rewards',self.rewards)
        returns = torch.tensor(returns)
        returns = (returns - returns.mean()) / (returns.std() + self.eps)

        # print("before policy_losses:", saved_actions, returns)
        for (log_prob, value), R in zip(saved_actions, returns):
            # advantage = R - value.item()
            advantage = R - value.mean()

            # calculate actor (policy) loss
            policy_losses.append(-log_prob * advantage)

            # calculate critic (value) loss using L1 smooth loss

            value_losses.append(F.smooth_l1_loss(value, torch.tensor([R] * 10).to(self.device)))

        # take gradient steps
        with torch.autograd.set_detect_anomaly(True):
            # self.optimizers['a_optimizer'].zero_grad()
            a_loss = torch.stack(policy_losses).sum()
            a_loss.backward(retain_graph=True)
            self.optimizers['a_optimizer'].step()

        # self.optimizers['c_optimizer'].zero_grad()
        v_loss = torch.stack(value_losses).sum()
        with torch.autograd.set_detect_anomaly(True):
            v_loss.backward()
            self.optimizers['c_optimizer'].step()

        # reset rewards and action buffer
        del self.rewards[:]
        del self.saved_actions[:]
        return - a_loss

    def configure_optimizers(self):
        optimizers = dict()
        actor_params = list(self.actor.parameters())
        critic_params = list(self.critic.parameters())
        optimizers['a_optimizer'] = torch.optim.Adam(actor_params, lr=learning_rate)
        optimizers['c_optimizer'] = torch.optim.Adam(critic_params, lr=learning_rate)
        return optimizers

    def save_checkpoint(self, path='ckpt.pth'):
        checkpoint = dict()
        checkpoint['model'] = self.state_dict()
        for key, value in self.optimizers.items():
            checkpoint[key] = value.state_dict()
        torch.save(checkpoint, path)

    def load_checkpoint(self, path='ckpt.pth'):
        checkpoint = torch.load(path)
        self.load_state_dict(checkpoint['model'])
        for key, value in self.optimizers.items():
            self.optimizers[key].load_state_dict(checkpoint[key])

    def log(self, log_dict, path='log.pth'):
        torch.save(log_dict, path)