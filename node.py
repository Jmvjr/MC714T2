import logging
import os
import signal
import threading
import time
from concurrent import futures
from dataclasses import dataclass

import grpc

import prot_pb2
import prot_pb2_grpc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)


class NodeLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prefix = f"node={self.extra['node_id']} clock={self.extra['clock']()}"
        return f"{prefix} {msg}", kwargs


@dataclass(frozen=True)
class Peer:
    node_id: int
    address: str


class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    @property
    def value(self):
        with self._lock:
            return self._value

    def tick(self):
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_timestamp):
        with self._lock:
            self._value = max(self._value, received_timestamp) + 1
            return self._value


class RequestQueue:
    def __init__(self):
        self._requests = []
        self._lock = threading.Lock()

    def add(self, timestamp, node_id):
        with self._lock:
            request = (timestamp, node_id)
            if request not in self._requests:
                self._requests.append(request)
                self._requests.sort()

    def remove(self, node_id):
        with self._lock:
            self._requests = [
                request for request in self._requests if request[1] != node_id
            ]

    def is_first(self, node_id):
        with self._lock:
            return bool(self._requests) and self._requests[0][1] == node_id

    def snapshot(self):
        with self._lock:
            return list(self._requests)


class NodeService(prot_pb2_grpc.NodeServicer):
    def __init__(self, node_id, clock, request_queue, logger):
        self.node_id = node_id
        self.clock = clock
        self.request_queue = request_queue
        self.logger = logger

    def Lock(self, request, context):
        current = self.clock.update(request.timestamp)
        self.request_queue.add(request.timestamp, request.node_id)
        self.logger.info(
            "recebeu LOCK de node=%s com timestamp=%s; fila=%s; respondeu timestamp=%s",
            request.node_id,
            request.timestamp,
            self.request_queue.snapshot(),
            current,
        )
        return prot_pb2.LockResponse(timestamp=current, node_id=self.node_id)

    def Unlock(self, request, context):
        current = self.clock.update(request.timestamp)
        self.request_queue.remove(request.node_id)
        self.logger.info(
            "recebeu UNLOCK de node=%s com timestamp=%s; fila=%s; respondeu timestamp=%s",
            request.node_id,
            request.timestamp,
            self.request_queue.snapshot(),
            current,
        )
        return prot_pb2.UnlockResponse(timestamp=current, node_id=self.node_id)


class LamportNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.clock = LamportClock()
        self.request_queue = RequestQueue()
        self.stop_event = threading.Event()
        self.logger = NodeLogAdapter(
            logging.getLogger(__name__),
            {"node_id": self.node_id, "clock": lambda: self.clock.value},
        )

    def start_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        prot_pb2_grpc.add_NodeServicer_to_server(
            NodeService(self.node_id, self.clock, self.request_queue, self.logger),
            server,
        )
        server.add_insecure_port(f"[::]:{self.port}")
        server.start()
        self.logger.info("servidor iniciado na porta %s", self.port)
        return server

    def request_lock(self):
        request_timestamp = self.clock.tick()
        self.request_queue.add(request_timestamp, self.node_id)
        self.logger.info("solicitando LOCK aos peers com timestamp=%s", request_timestamp)

        replies = set()
        for peer in self.peers:
            try:
                with grpc.insecure_channel(peer.address) as channel:
                    stub = prot_pb2_grpc.NodeStub(channel)
                    response = stub.Lock(
                        prot_pb2.LockRequest(
                            timestamp=request_timestamp,
                            node_id=self.node_id,
                        ),
                        timeout=5,
                    )
                self.clock.update(response.timestamp)
                replies.add(response.node_id)
                self.logger.info(
                    "recebeu resposta LOCK de node=%s com timestamp=%s",
                    response.node_id,
                    response.timestamp,
                )
            except grpc.RpcError as exc:
                self.logger.warning(
                    "falha ao solicitar LOCK para node=%s em %s: %s",
                    peer.node_id,
                    peer.address,
                    exc.details(),
                )

        expected_replies = {peer.node_id for peer in self.peers}
        if replies != expected_replies:
            self.logger.warning(
                "nao entrou na regiao critica; respostas recebidas=%s esperadas=%s",
                sorted(replies),
                sorted(expected_replies),
            )
            self.request_queue.remove(self.node_id)
            return False

        while not self.request_queue.is_first(self.node_id):
            self.logger.info("aguardando topo da fila; fila=%s", self.request_queue.snapshot())
            time.sleep(0.5)

        self.clock.tick()
        self.logger.info("entrou na regiao critica; fila=%s", self.request_queue.snapshot())
        return True

    def release_lock(self):
        release_timestamp = self.clock.tick()
        self.request_queue.remove(self.node_id)
        self.logger.info("liberando LOCK com timestamp=%s", release_timestamp)

        for peer in self.peers:
            try:
                with grpc.insecure_channel(peer.address) as channel:
                    stub = prot_pb2_grpc.NodeStub(channel)
                    response = stub.Unlock(
                        prot_pb2.UnlockRequest(
                            timestamp=release_timestamp,
                            node_id=self.node_id,
                        ),
                        timeout=5,
                    )
                self.clock.update(response.timestamp)
                self.logger.info(
                    "recebeu resposta UNLOCK de node=%s com timestamp=%s",
                    response.node_id,
                    response.timestamp,
                )
            except grpc.RpcError as exc:
                self.logger.warning(
                    "falha ao enviar UNLOCK para node=%s em %s: %s",
                    peer.node_id,
                    peer.address,
                    exc.details(),
                )

        self.clock.tick()
        self.logger.info("saiu da regiao critica")

    def run_demo(self, delay, hold_time):
        time.sleep(delay)
        locked = self.request_lock()
        if locked:
            time.sleep(hold_time)
            self.release_lock()


def parse_peers(peers_config):
    peers = []
    if not peers_config:
        return peers

    for item in peers_config.split(","):
        node_id, address = item.split("=", 1)
        peers.append(Peer(node_id=int(node_id), address=address))
    return peers


def env_int(name, default):
    return int(os.environ.get(name, default))


def main():
    node_id = env_int("NODE_ID", 1)
    port = env_int("NODE_PORT", 50051)
    peers = parse_peers(os.environ.get("PEERS", ""))

    node = LamportNode(node_id=node_id, port=port, peers=peers)
    server = node.start_server()

    def stop(*_):
        node.stop_event.set()
        server.stop(grace=2)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    if os.environ.get("AUTO_DEMO", "false").lower() == "true":
        delay = env_int("DEMO_DELAY", 3)
        hold_time = env_int("DEMO_HOLD_TIME", 5)
        threading.Thread(
            target=node.run_demo,
            args=(delay, hold_time),
            daemon=True,
        ).start()

    while not node.stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
