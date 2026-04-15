import click
import grpc
import os
import math

from common.config import CHUNK_SIZE
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc
from proto.master_pb2_grpc import MasterServiceStub

MASTER_ADDRESS = "localhost:5050"

def translateAddress(dockerAddress: str) -> str:
    port = dockerAddress.split(":")[1]
    return f"localhost:{port}"

@click.group()
def cli():
    pass

@cli.command()
@click.argument("filepath")
@click.option("--name", default = None, help = "Name to store as in DFS; Defaults to filename")
def upload(filepath, name):
    if not os.path.exists(filepath):
        click.echo(f"Error: File {filepath} does not exist")
        return
    
    if name is None:
        name = os.path.basename(filepath)
    
    with open(filepath, 'rb') as file:
        dataBytes = file.read()

    channel = grpc.insecure_channel(MASTER_ADDRESS)
    stub = MasterServiceStub(channel)
    try:
        response: master_pb2.UploadFileResponse = stub.UploadFile(
            master_pb2.UploadFileRequest(
                filename = name, 
                file_size = len(dataBytes)
                ))
    except grpc.RpcError as e:
        click.echo(f"Error: {e.details()}")
        return
    
    for assignment in response.assignments:
        chunkIndex = assignment.chunk_index
        chunkStart = chunkIndex * CHUNK_SIZE
        chunkEnd = min(chunkStart + CHUNK_SIZE, len(dataBytes))
        chunkData = dataBytes[ chunkStart: chunkEnd]

        addresses = list(assignment.server_addresses)
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]

        chunkChannel = grpc.insecure_channel(primaryAddress)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)

        STREAM_SIZE = 512 * 1024

        def makeIterator(chunk, chunkHandle, forwards):
            for i in range(0, len(chunk), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle = chunkHandle,
                    data = chunk[i:i+STREAM_SIZE],
                    forward_addresses = forwards
                )
        writeResponse = chunkStub.WriteChunk(makeIterator(chunkData, assignment.chunk_handle, forwardAddresses))
        if not writeResponse.success:
            click.echo(f"Error writing chunk {chunkIndex}: {writeResponse.message}")
            return
    click.echo(f"Uploaded '{name}' ({len(dataBytes)} bytes, {len(response.assignments)} chunks)")

    return

if __name__ == "__main__":
    cli()