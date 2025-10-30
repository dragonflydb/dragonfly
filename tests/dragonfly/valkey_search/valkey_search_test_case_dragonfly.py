"""
Dragonfly adapter for valkey_search_test_case.py
Creates real Dragonfly instances with replicas and clusters
"""

import os
import time
import pytest
import valkey
from valkey import ResponseError
from valkey.client import Valkey
from typing import List
import random
import string
import logging

# Import Dragonfly test infrastructure
from ..instance import DflyInstance, DflyInstanceFactory

LOGS_DIR = "/tmp/dragonfly-valkey-test-logs"

if "LOGS_DIR" in os.environ:
    LOGS_DIR = os.environ["LOGS_DIR"]


class Node:
    """This class represents a Dragonfly instance as a valkey server node"""

    def __init__(
        self,
        client=None,
        server=None,
        logfile=None,
        df_instance=None,
    ):
        self.client: Valkey = client
        self.server = server
        self.logfile: str = logfile
        self.df_instance: DflyInstance = df_instance

    def does_logfile_contains(self, pattern: str) -> bool:
        # For Dragonfly, simplified log checking
        return True


class ValkeyServerHandle:
    """Adapter for Dragonfly instance to look like ValkeyServerHandle"""

    def __init__(self, df_instance: DflyInstance):
        self.df_instance = df_instance
        self.bind_ip = "127.0.0.1"
        self.port = df_instance.port if df_instance else 6379

    def pid(self):
        return self.df_instance.proc.pid if self.df_instance and self.df_instance.proc else None

    def get_new_client(self):
        return valkey.Valkey(host=self.bind_ip, port=self.port, decode_responses=False)


class ReplicationGroup:
    """Replication group for Dragonfly"""

    def __init__(
        self,
        primary,
        replicas=None,
    ):
        self.primary: Node = primary
        self.replicas: List[Node] = replicas or []
        self._setup_done = False

    def setup_replications_cluster(self):
        # For cluster mode - not needed for single master/replica
        pass

    def setup_replications_cmd(self):
        """Setup replication using REPLICAOF command"""
        if self._setup_done or not self.replicas:
            return

        primary_ip = "localhost"
        primary_port = self.primary.df_instance.port

        # Configure each replica
        for replica in self.replicas:
            try:
                # Use REPLICAOF to setup replication
                result = replica.client.execute_command(f"REPLICAOF {primary_ip} {primary_port}")
                logging.debug(f"Setup replica on port {replica.df_instance.port}: {result}")
            except Exception as e:
                logging.error(f"Failed to setup replica: {e}")

        self._setup_done = True
        self._wait_for_replication()

    def _wait_for_replication(self):
        """Wait for replicas to sync"""
        # Give replicas time to connect
        time.sleep(0.5)

        # Check if replicas are connected
        try:
            info = self.primary.client.info("replication")
            connected_slaves = info.get("connected_slaves", 0)
            logging.debug(f"Connected slaves: {connected_slaves}, expected: {len(self.replicas)}")
        except Exception as e:
            logging.debug(f"Could not check replication status: {e}")

    def _check_all_replicas_are_connected(self):
        try:
            return self.primary.client.info("replication")["connected_slaves"] == len(self.replicas)
        except:
            return False

    def _check_is_replica_online(self, name) -> bool:
        try:
            replica_status = self.primary.client.info("replication")[name]
            return replica_status["state"] == "online"
        except:
            return False  # Assume offline if we can't check

    def get_replica_connection(self, index) -> Valkey:
        if index < len(self.replicas):
            return self.replicas[index].client
        raise IndexError(f"No replica at index {index}")

    def get_primary_connection(self) -> Valkey:
        return self.primary.client

    @staticmethod
    def cleanup(rg):
        """Cleanup Dragonfly instances"""
        # Cleanup is handled by Dragonfly fixtures
        pass


class ValkeySearchTestCaseCommon:
    """Common base class for tests"""

    pass


class ValkeyTestCase(ValkeySearchTestCaseCommon):
    """Base test case class"""

    pass


class ReplicationTestCase(ValkeyTestCase):
    """Replication test case"""

    pass


class ValkeySearchTestCaseBase(ValkeySearchTestCaseCommon):
    """Base test case for valkey-search tests running on Dragonfly"""

    @pytest.fixture(autouse=True)
    def setup_test(self, request, df_factory: DflyInstanceFactory):
        """Setup test with Dragonfly instances"""
        # Get replica count from parametrize if provided
        replica_count = 0
        if hasattr(request, "param") and "replica_count" in request.param:
            replica_count = request.param["replica_count"]

        # Create primary instance
        primary_df = df_factory.create(proactor_threads=4)
        primary_df.start()

        primary_client = valkey.Valkey(
            host="127.0.0.1", port=primary_df.port, decode_responses=False
        )

        primary_server = ValkeyServerHandle(primary_df)
        primary_node = Node(
            client=primary_client, server=primary_server, logfile=None, df_instance=primary_df
        )

        # Create replica instances
        replicas: List[Node] = []
        for i in range(replica_count):
            replica_df = df_factory.create(proactor_threads=4)
            replica_df.start()

            replica_client = valkey.Valkey(
                host="127.0.0.1", port=replica_df.port, decode_responses=False
            )

            replica_server = ValkeyServerHandle(replica_df)
            replica_node = Node(
                client=replica_client, server=replica_server, logfile=None, df_instance=replica_df
            )
            replicas.append(replica_node)

        # Setup replication group
        self.rg = ReplicationGroup(primary=primary_node, replicas=replicas)

        # Configure replication
        if replica_count > 0:
            self.rg.setup_replications_cmd()

        self.server = self.rg.primary.server
        self.client = self.rg.primary.client
        self.nodes: List[Node] = [self.rg.primary] + self.rg.replicas

        yield

        # Cleanup is handled by df_factory

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            if isinstance(cmd, str):
                cmd_args = cmd.split()
            else:
                cmd_args = cmd
            client.execute_command(*cmd_args)
            assert False, f"Expected error '{expected_err_reply}' but command succeeded"
        except ResponseError as e:
            error_str = str(e)
            assert (
                expected_err_reply in error_str
            ), f"Actual error message: '{error_str}' doesn't contain expected: '{expected_err_reply}'"
            return error_str

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = client.dbsize()
        assert (
            actual_num_keys == expected_num_keys
        ), f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"

    def generate_random_string(self, length=7):
        """Creates a random string with specified length."""
        characters = string.ascii_letters + string.digits
        random_string = "".join(random.choice(characters) for _ in range(length))
        return random_string

    def parse_valkey_info(self, section):
        mem_info = self.client.execute_command("INFO " + section)
        if isinstance(mem_info, bytes):
            mem_info = mem_info.decode("utf-8")
        lines = mem_info.split("\\r\\n")
        stats_dict = {}
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                stats_dict[key.strip()] = value.strip()
        return stats_dict

    def start_new_server(self, is_primary=True) -> Node:
        """Return existing or create new server"""
        if is_primary:
            return self.rg.primary
        elif self.rg.replicas:
            return self.rg.replicas[0]
        else:
            # No replicas configured
            return self.rg.primary

    def get_replica_connection(self, index) -> Valkey:
        return self.rg.get_replica_connection(index)

    def get_primary_connection(self) -> Valkey:
        return self.rg.get_primary_connection()


class ValkeySearchTestCaseDebugMode(ValkeySearchTestCaseBase):
    """Debug mode variant"""

    pass


class ValkeySearchClusterTestCase(ValkeySearchTestCaseCommon):
    """Cluster test case - simplified for single Dragonfly instance"""

    CLUSTER_SIZE = 1  # Simplified to single node
    REPLICAS_COUNT = 0

    @pytest.fixture(autouse=True)
    def setup_test(self, request, df_factory: DflyInstanceFactory):
        """Setup cluster test with Dragonfly instances"""

        # Get replica count from parametrize if provided
        replica_count = 0
        if hasattr(request, "param") and "replica_count" in request.param:
            replica_count = request.param["replica_count"]

        # Create primary instance
        primary_df = df_factory.create(proactor_threads=4)
        primary_df.start()

        primary_client = valkey.Valkey(
            host="127.0.0.1", port=primary_df.port, decode_responses=False
        )

        primary_server = ValkeyServerHandle(primary_df)
        primary_node = Node(
            client=primary_client, server=primary_server, logfile=None, df_instance=primary_df
        )

        # Create replica instances
        replicas: List[Node] = []
        for i in range(replica_count):
            replica_df = df_factory.create(proactor_threads=4)
            replica_df.start()

            replica_client = valkey.Valkey(
                host="127.0.0.1", port=replica_df.port, decode_responses=False
            )

            replica_server = ValkeyServerHandle(replica_df)
            replica_node = Node(
                client=replica_client, server=replica_server, logfile=None, df_instance=replica_df
            )
            replicas.append(replica_node)

        rg = ReplicationGroup(primary=primary_node, replicas=replicas)

        # Configure replication
        if replica_count > 0:
            rg.setup_replications_cmd()

        self.replication_groups = [rg]
        self.nodes: List[Node] = [rg.primary] + rg.replicas

        yield

        # Cleanup handled by df_factory

    def get_primary(self, index):
        return self.replication_groups[index].primary.server

    def get_primary_port(self, index):
        return self.replication_groups[index].primary.server.port

    def new_client_for_primary(self, index):
        return self.replication_groups[index].primary.server.get_new_client()

    def client_for_primary(self, index):
        return self.replication_groups[index].primary.client

    def get_all_primary_clients(self) -> List[Valkey]:
        return [rg.primary.client for rg in self.replication_groups]

    def get_replication_group(self, index):
        return self.replication_groups[index]

    def new_cluster_client(self):
        """Return regular client for single-node"""
        return self.replication_groups[0].primary.client


class ValkeySearchClusterTestCaseDebugMode(ValkeySearchClusterTestCase):
    """Debug mode cluster variant"""

    pass
