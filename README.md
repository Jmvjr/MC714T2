# Relogio de Lamport com gRPC

Este projeto cria tres processos em Docker, cada um com seu proprio relogio de Lamport.

## Subir os nos

```bash
docker compose up --build
```

O `node1` e o `node2` executam uma demonstracao automatica:

- incrementam o relogio antes de pedir o lock;
- enviam `LockRequest` para os peers;
- atualizam o relogio com `max(local, recebido) + 1`;
- entram na regiao critica;
- enviam `UnlockRequest` ao sair.

## Portas

- `node1`: `localhost:50051`
- `node2`: `localhost:50052`
- `node3`: `localhost:50053`

Dentro da rede Docker, os peers se comunicam usando os nomes dos servicos:

- `node1:50051`
- `node2:50051`
- `node3:50051`

## Gerar os arquivos Python manualmente

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. prot.proto
```
