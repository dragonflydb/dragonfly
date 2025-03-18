from time import sleep

from ducktape.cluster.cluster import ClusterNode
from ducktape.services.service import Service
from ducktape.tests.test import Test


class MyService(Service):
    def __init__(self, context, num_nodes):
        super().__init__(context, num_nodes)

    def start_node(self, node: ClusterNode, **kwargs):
        command = "nohup dragonfly >/dev/null 2>&1 </dev/null &"
        self.logger.info(f"starting on {self.idx(node)}")
        node.account.ssh(command)
        self.logger.info(f"started on {self.idx(node)}")

    def stop_node(self, node: ClusterNode, **kwargs):
        node.account.kill_process("dragonfly", allow_fail=False)


class MyTest(Test):
    def __init__(self, test_context):
        super().__init__(test_context)
        self.svc = MyService(test_context, num_nodes=1)

    def test_foo(self):
        self.svc.start()
        sleep(5)
