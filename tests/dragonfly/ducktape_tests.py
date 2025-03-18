from pprint import pprint
from time import sleep

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.tests.test import Test


class MyService(Service):
    def __init__(self, context, num_nodes):
        super().__init__(context, num_nodes)
        self.global_ctx = context.globals

    def start_node(self, node: ClusterNode, **kwargs):
        command = "nohup dragonfly --port {port} >/dev/null 2>&1 </dev/null &"
        hostname = node.account.hostname

        node_ctx = None
        for node_info in self.global_ctx["nodes"]:
            if node_info["hostname"] == hostname:
                node_ctx = node_info

        assert node_ctx is not None

        self.logger.info(f"starting on {node.account.hostname}, info is {node_ctx}")
        node.account.ssh(command.format(port=node_ctx["app_port"]))
        self.logger.info(f"started on {node.account.hostname}")

    def stop_node(self, node: ClusterNode, **kwargs):
        node.account.kill_process("dragonfly", allow_fail=False)


class MyTest(Test):
    def __init__(self, test_context):
        super().__init__(test_context)
        self.svc = MyService(test_context, num_nodes=1)

    def test_foo(self):
        self.svc.start()
        sleep(120)
