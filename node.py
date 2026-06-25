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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


class NodeLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prefix = f"node={self.extra['node_id']} clock={self.extra['clock']()} leader={self.extra['leader']()}"
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
            self._requests = [r for r in self._requests if r[1] != node_id]

    def is_first(self, node_id):
        with self._lock:
            return bool(self._requests) and self._requests[0][1] == node_id


# -------------------------------------------------------------------
# SERVICER: Respostas gRPC recebidas da rede
# -------------------------------------------------------------------
class NodeService(prot_pb2_grpc.NodeServicer):
    def __init__(self, node):
        self.node = node

    def Lock(self, request, context):
        self.node.clock.update(request.timestamp)
        if self.node.leader_id != self.node.node_id:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return prot_pb2.LockResponse()

        self.node.request_queue.add(request.timestamp, request.node_id)

        # CORREÇÃO: Acessa diretamente a lista interna clonando-a com list()
        atual_fila = list(self.node.request_queue._requests)
        self.node.logger.info(
            "LÍDER recebeu LOCK de node=%s | Fila Atual: %s",
            request.node_id,
            atual_fila,
        )

        while not self.node.request_queue.is_first(request.node_id):
            time.sleep(0.1)
            if self.node.stop_event.is_set():
                break

        return prot_pb2.LockResponse(
            timestamp=self.node.clock.value, node_id=self.node.node_id
        )

    def Unlock(self, request, context):
        self.node.clock.update(request.timestamp)
        self.node.request_queue.remove(request.node_id)

        # CORREÇÃO: Acessa diretamente a lista interna clonando-a com list()
        atual_fila = list(self.node.request_queue._requests)
        self.node.logger.info(
            "LÍDER recebeu UNLOCK de node=%s | Fila Atual: %s",
            request.node_id,
            atual_fila,
        )

        return prot_pb2.UnlockResponse(
            timestamp=self.node.clock.value, node_id=self.node.node_id
        )

    def Election(self, request, context):
        self.node.clock.update(request.timestamp)
        self.node.logger.info("Recebeu ELECTION de node=%s", request.node_id)

        if request.node_id < self.node.node_id:
            threading.Thread(target=self.node.start_election, daemon=True).start()

        return prot_pb2.ElectionResponse(
            timestamp=self.node.clock.value, node_id=self.node.node_id
        )

    def Coordinator(self, request, context):
        self.node.clock.update(request.timestamp)
        self.node.leader_id = request.leader_id
        self.node.leader_address = request.leader_address
        self.node.logger.info(
            "Novo lider declarado: node=%s em %s",
            request.leader_id,
            request.leader_address,
        )
        return prot_pb2.CoordinatorResponse(timestamp=self.node.clock.value)


# -------------------------------------------------------------------
# LAMPORT NODE: Lógica ativa de execução do Nó
# -------------------------------------------------------------------
class LamportNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.clock = LamportClock()
        self.request_queue = RequestQueue()
        self.stop_event = threading.Event()
        self.leader_id = None
        self.leader_address = None

        self.logger = NodeLogAdapter(
            logging.getLogger(__name__),
            {
                "node_id": self.node_id,
                "clock": lambda: self.clock.value,
                "leader": lambda: self.leader_id,
            },
        )

    def start_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=30))
        prot_pb2_grpc.add_NodeServicer_to_server(NodeService(self), server)
        server.add_insecure_port(f"[::]:{self.port}")
        server.start()
        self.logger.info("servidor iniciado na porta %s", self.port)
        return server

    def start_election(self):
        self.logger.info("Iniciando processo de eleicao (Algoritmo do Valentao)...")
        higher_peers = [p for p in self.peers if p.node_id > self.node_id]
        answered_ok = False
        election_timestamp = self.clock.tick()

        for peer in higher_peers:
            try:
                with grpc.insecure_channel(peer.address) as channel:
                    stub = prot_pb2_grpc.NodeStub(channel)
                    response = stub.Election(
                        prot_pb2.ElectionRequest(
                            timestamp=election_timestamp, node_id=self.node_id
                        ),
                        timeout=1,
                    )
                    self.clock.update(response.timestamp)
                    answered_ok = True
            except grpc.RpcError:
                continue

        if not answered_ok:
            self.logger.info("Nenhum no maior respondeu. Eu sou o novo Lider!")
            self.leader_id = self.node_id
            self.leader_address = f"node{self.node_id}:{self.port}"

            coord_timestamp = self.clock.tick()
            for peer in self.peers:
                try:
                    with grpc.insecure_channel(peer.address) as channel:
                        stub = prot_pb2_grpc.NodeStub(channel)
                        stub.Coordinator(
                            prot_pb2.CoordinatorRequest(
                                timestamp=coord_timestamp,
                                leader_id=self.node_id,
                                leader_address=self.leader_address,
                            ),
                            timeout=1,
                        )
                except grpc.RpcError:
                    pass

    def run_demo(self, delay):
        time.sleep(delay)

        if self.leader_id is None:
            self.start_election()

        # Loop de disputa periódica pela Seção Crítica
        while not self.stop_event.is_set():
            time.sleep(5)  # Aguarda 5 segundos entre tentativas de acesso

            if self.leader_address:
                try:
                    self.logger.info(
                        "Solicitando LOCK para o Líder (%s)", self.leader_id
                    )
                    lock_timestamp = self.clock.tick()

                    with grpc.insecure_channel(self.leader_address) as channel:
                        stub = prot_pb2_grpc.NodeStub(channel)
                        # Chamada bloqueante: só retorna quando o líder der a outorga
                        response = stub.Lock(
                            prot_pb2.LockRequest(
                                timestamp=lock_timestamp, node_id=self.node_id
                            ),
                            timeout=15,
                        )
                        self.clock.update(response.timestamp)

                    # === INÍCIO DA SEÇÃO CRÍTICA ===
                    self.logger.info("=== Executando Seção Crítica ===")
                    time.sleep(10)  # Simula trabalho na região crítica
                    # === FIM DA SEÇÃO CRÍTICA ===

                    self.logger.info("Liberando LOCK com o Líder")
                    unlock_timestamp = self.clock.tick()
                    with grpc.insecure_channel(self.leader_address) as channel:
                        stub = prot_pb2_grpc.NodeStub(channel)
                        response = stub.Unlock(
                            prot_pb2.UnlockRequest(
                                timestamp=unlock_timestamp, node_id=self.node_id
                            ),
                            timeout=5,
                        )
                        self.clock.update(response.timestamp)

                except grpc.RpcError:
                    self.logger.warning(
                        "Líder indisponível ou falhou durante operação de exclusão mútua."
                    )
                    self.leader_id = None
                    self.leader_address = None
                    self.start_election()


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
        delay = env_int("DEMO_DELAY", 5)
        threading.Thread(target=node.run_demo, args=(delay,), daemon=True).start()

    while not node.stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
