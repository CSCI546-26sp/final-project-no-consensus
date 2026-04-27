import click
import grpc
import os
from concurrent.futures import ThreadPoolExecutor

from common.config import CHUNK_SIZE, GRPC_OPTIONS
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc
from proto.master_pb2_grpc import MasterServiceStub

MASTER_ADDRESS = "localhost:5050"
UPLOAD_WORKERS = 4
STREAM_SIZE = 512 * 1024

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

    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
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
    
    def uploadOneChunk(assignment):
        chunkIndex = assignment.chunk_index
        chunkStart = chunkIndex * CHUNK_SIZE
        chunkEnd = min(chunkStart + CHUNK_SIZE, len(dataBytes))
        chunkData = dataBytes[chunkStart:chunkEnd]

        addresses = list(assignment.server_addresses)
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]

        chunkChannel = grpc.insecure_channel(primaryAddress, options=GRPC_OPTIONS)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)

        def makeIterator():
            for i in range(0, len(chunkData), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle = assignment.chunk_handle,
                    data = chunkData[i:i+STREAM_SIZE],
                    forward_addresses = forwardAddresses,
                )

        writeResponse = chunkStub.WriteChunk(makeIterator())
        if not writeResponse.success:
            raise RuntimeError(f"chunk {chunkIndex}: {writeResponse.message}")

    try:
        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as ex:
            list(ex.map(uploadOneChunk, response.assignments))
    except Exception as e:
        click.echo(f"Error writing chunk: {e}")
        return
    click.echo(f"Uploaded '{name}' ({len(dataBytes)} bytes, {len(response.assignments)} chunks)")

    return

@cli.command()
@click.argument("filename")
@click.option("--output", default = None, help = "local path to save the file; defaults to filename")
def download(filename, output):
    if output is None:
        output = filename
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    masterStub = MasterServiceStub(channel)

    try:
        response = masterStub.DownloadFile(
            master_pb2.DownloadFileRequest(filename = filename)
        )
    except grpc.RpcError as e:
        click.echo(f"Error: {e.details()}")
        return

    locations = sorted(response.locations, key = lambda x: x.chunk_index)
    fileData = bytearray()

    for location in locations:
        address = translateAddress(location.server_addresses[0])
        chunkChannel = grpc.insecure_channel(address, options=GRPC_OPTIONS)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)
        try:
            for readResponse in chunkStub.ReadChunk(
                chunkserver_pb2.ReadChunkRequest(chunk_handle = location.chunk_handle)
            ):
                fileData.extend(readResponse.data)
        except grpc.RpcError as e:
            click.echo(f"Error reading chunk {location.chunk_index}: {e.details()}")
            return
    
    with open(output, "wb") as f:
        f.write(fileData)
    
    click.echo(f"Downloaded '{filename}' ({len(fileData)} bytes, {len(locations)} chunks) -> {output}")

@cli.command("list")
def listFiles():
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    stub = MasterServiceStub(channel)

    try:
        response = stub.ListFiles(master_pb2.ListFilesRequest())
    except grpc.RpcError as e:
        click.echo(f"Error while listing files: {e.details()}")
        return
    
    if not response.files:
        click.echo("No files found")
        return
    click.echo(f"{'Filename':<40} {'Size':>12} {'Chunks':>8}")
    click.echo("-"*62)

    for f in response.files:
        click.echo(f"{f.filename:<40} {f.file_size:>12} {f.num_chunks:>8}")

@cli.command()
@click.argument("filename")
def delete(filename):
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    stub = MasterServiceStub(channel)
    try:
        response = stub.DeleteFile(master_pb2.DeleteFileRequest(filename = filename))
        click.echo(f"Deleted: {filename}")
    except grpc.RpcError as e:
        click.echo(f"Error while deleting files: {e.details()}")
        return
    
@cli.command()
@click.argument("query")
def search(query):
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    stub = MasterServiceStub(channel)
    try:
        response = stub.SearchFiles(master_pb2.SearchFilesRequest(query = query))
    except grpc.RpcError as e:
        click.echo(f"Error while searching: {e.details()}")
        return
    
    if not response.results:
        click.echo("No results found")
        return

    click.echo(f"{'Filename':<40} {'Score':>8} {'Line':>6}  Snippet")
    click.echo("-" * 80)
    for r in response.results:
        click.echo(f"{r.filename:<40} {r.score:>8.2f} {r.line_number:>6} {r.snippet}")

@cli.command()
@click.argument("filename")
@click.argument("query")
def filesearch(filename, query):
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    stub = MasterServiceStub(channel)
    try:
        response = stub.FileSearch(
            master_pb2.FileSearchRequest(filename = filename, query = query)
        )
    except grpc.RpcError as e:
        click.echo(f"Error: {e.details()}")
        return

    if not response.matches:
        click.echo(f"No matches for '{query}' in '{filename}'")

    click.echo(f"Matches for '{query}' in '{filename}' ({len(response.matches)} hits):")
    click.echo(f"{'Chunk':>6} {'Line':>6}  Text")                                 
    click.echo("-" * 80)                                                          
    for m in response.matches:                                                    
        click.echo(f"{m.chunk_index:>6} {m.line_number:>6}  {m.line_text}") 

if __name__ == "__main__":
    cli()