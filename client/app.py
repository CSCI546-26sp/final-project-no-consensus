"""
Flask web UI for the Distributed File System client.
Run from project root: python -m client.app
"""

import grpc
import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Semaphore
from flask import Flask, render_template, request, jsonify, send_file

from common.config import CHUNK_SIZE, GRPC_OPTIONS
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc
from proto.master_pb2_grpc import MasterServiceStub

app = Flask(__name__)

MASTER_ADDRESS = "localhost:5050"
UPLOAD_WORKERS = 4
STREAM_SIZE = 512 * 1024


def translateAddress(dockerAddress: str) -> str:
    port = dockerAddress.split(":")[1]
    return f"localhost:{port}"


def getMasterStub():
    channel = grpc.insecure_channel(MASTER_ADDRESS, options=GRPC_OPTIONS)
    return MasterServiceStub(channel)


def formatSize(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


@app.route("/")
def index():
    path = os.path.join(os.path.dirname(__file__), "templates", "Distributed File System.html")
    return send_file(path)


@app.route("/api/files")
def apiListFiles():
    try:
        stub = getMasterStub()
        response = stub.ListFiles(master_pb2.ListFilesRequest())
        files = []
        for f in response.files:
            files.append({
                "filename": f.filename,
                "file_size": f.file_size,
                "file_size_human": formatSize(f.file_size),
                "num_chunks": f.num_chunks,
                "chunks": [{
                    "handle": c.chunk_handle,
                    "index": c.chunk_index,
                    "server_ids": list(c.server_ids),
                } for c in f.chunks],
            })
        return jsonify(files)
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500


@app.route("/api/upload", methods=["POST"])
def apiUpload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    name = request.form.get("name", "").strip()
    if not name:
        name = file.filename

    # Werkzeug already spooled big uploads to a SpooledTemporaryFile. Don't
    # slurp the whole thing into memory — stream from file.stream in 512 KB
    # pieces so peak resident stays bounded regardless of upload size.
    stream = file.stream
    stream.seek(0, 2)
    size = stream.tell()
    stream.seek(0)

    try:
        masterStub = getMasterStub()
        response = masterStub.UploadFile(
            master_pb2.UploadFileRequest(filename=name, file_size=size)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 400

    def uploadChunk(assignment, chunkBytes):
        addresses = list(assignment.server_addresses)
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]

        chunkChannel = grpc.insecure_channel(primaryAddress, options=GRPC_OPTIONS)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)

        def iterator():
            for i in range(0, len(chunkBytes), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle=assignment.chunk_handle,
                    data=chunkBytes[i:i + STREAM_SIZE],
                    forward_addresses=forwardAddresses,
                )

        writeResponse = chunkStub.WriteChunk(iterator())
        if not writeResponse.success:
            raise RuntimeError(f"Chunk {assignment.chunk_index} write failed: {writeResponse.message}")

    # Bound in-flight chunks so peak resident is workers × CHUNK_SIZE (~16 MB at 4 × 4 MB),
    # not the full upload size. Main thread reads each chunk into a buffer, blocking on the
    # semaphore so it never reads ahead of worker capacity.
    sortedAssignments = sorted(response.assignments, key=lambda a: a.chunk_index)
    sem = Semaphore(UPLOAD_WORKERS)
    futures = []
    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as ex:
        for assignment in sortedAssignments:
            sem.acquire()
            chunkIndex = assignment.chunk_index
            thisChunkSize = min(CHUNK_SIZE, size - chunkIndex * CHUNK_SIZE)
            chunkBytes = stream.read(thisChunkSize)
            if len(chunkBytes) != thisChunkSize:
                sem.release()
                return jsonify({"error": "Upload stream ended unexpectedly"}), 500
            fut = ex.submit(uploadChunk, assignment, chunkBytes)
            fut.add_done_callback(lambda _: sem.release())
            futures.append(fut)

        for fut in futures:
            err = fut.exception()
            if err is not None:
                if isinstance(err, grpc.RpcError):
                    return jsonify({"error": f"Upload failed: {err.details()}"}), 500
                return jsonify({"error": str(err)}), 500

    return jsonify({
        "message": f"Uploaded '{name}' ({formatSize(size)}, {len(response.assignments)} chunks)",
        "filename": name,
        "size": size,
        "chunks": len(response.assignments),
    })


@app.route("/api/download/<path:filename>")
def apiDownload(filename):
    try:
        masterStub = getMasterStub()
        response = masterStub.DownloadFile(
            master_pb2.DownloadFileRequest(filename=filename)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 400

    locations = sorted(response.locations, key=lambda x: x.chunk_index)
    fileData = bytearray()

    for location in locations:
        address = translateAddress(location.server_addresses[0])
        chunkChannel = grpc.insecure_channel(address, options=GRPC_OPTIONS)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)
        try:
            for readResponse in chunkStub.ReadChunk(
                chunkserver_pb2.ReadChunkRequest(chunk_handle=location.chunk_handle)
            ):
                fileData.extend(readResponse.data)
        except grpc.RpcError as e:
            return jsonify({"error": f"Error reading chunk {location.chunk_index}: {e.details()}"}), 500

    return send_file(
        BytesIO(bytes(fileData)),
        download_name=filename,
        as_attachment=True,
    )


@app.route("/api/files/<path:filename>", methods=["DELETE"])
def apiDeleteFile(filename):
    try:
        stub = getMasterStub()
        response = stub.DeleteFile(
            master_pb2.DeleteFileRequest(filename=filename)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 400

    return jsonify({"message": f"Deleted '{filename}'"})


@app.route("/api/search")
def apiSearch():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "No search query provided"}), 400

    try:
        stub = getMasterStub()
        t0 = time.perf_counter()
        response = stub.SearchFiles(
            master_pb2.SearchFilesRequest(query=query)
        )
        latency_ms = (time.perf_counter() - t0) * 1000
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    results = []
    for r in response.results:
        results.append({
            "filename": r.filename,
            "score": round(r.score, 4),
            "line_number": r.line_number,
            "snippet": r.snippet,
            "chunk_handle": r.chunk_handle,
        })

    return jsonify({"results": results, "latency_ms": latency_ms})


@app.route("/api/filesearch")
def apiFileSearch():
    filename = request.args.get("filename", "").strip()
    query = request.args.get("q", "").strip()
    if not filename or not query:
        return jsonify({"error": "Filename and query are required"}), 400

    try:
        stub = getMasterStub()
        response = stub.FileSearch(
            master_pb2.FileSearchRequest(filename=filename, query=query)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 400

    matches = []
    for m in response.matches:
        matches.append({
            "chunk_index": m.chunk_index,
            "line_number": m.line_number,
            "line_text": m.line_text,
        })
    return jsonify(matches)


@app.route("/api/index-status/<path:filename>")
def apiIndexStatus(filename):
    try:
        stub = getMasterStub()
        response = stub.FileIndexStatus(
            master_pb2.FileIndexStatusRequest(filename=filename)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 404
    return jsonify({
        "indexed_chunks": response.indexed_chunks,
        "total_chunks": response.total_chunks,
        "done": response.done,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
