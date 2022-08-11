from typing import List, Dict


# The smallest unit of deployment target. It is comprised of a single
# network port and its fair share of cores.
class HostNode:
    def __init__(self, dns: str, fastip: str, cores: List[int], rdma: str, config: str):
        """
        :param dns: DNS name of host
        :param fastip: IP address of the network port to listen on
        :param cores: List of core IDs to use
        :param rdma: Name of RDMA port to use, e.g. "mlx5_0"
        :param config: Hardware config of the server
        """
        self.dns = dns
        self.fastip = fastip
        self.cores = cores
        self.rdma = rdma
        self.config = config


host_nodes: List[HostNode] = [
    HostNode("skv-node2", "192.168.1.103", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [20, 21, 22, 23, 24, 25, 26, 27, 28, 29], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [30, 31, 32, 33, 34, 35, 36, 37, 38, 39], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [40, 41, 42, 43, 44, 45, 46, 47, 48, 49], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [50, 51, 52, 53, 54, 55, 56, 57, 58, 59], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [60, 61, 62, 63, 64, 65, 66, 67, 68, 69], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [70, 71, 72, 73, 74, 75, 76, 77, 78, 79], "mlx5_1", "A"),
    HostNode("skv-node2", "192.168.1.103", [3, 4, 5, 6, 7, 8, 9], "mlx5_1", "A")]

port_bases: Dict[str, int] = {"cpo": 12300, "tso": 12400, "persist": 12500, "nodepool": 12600}
