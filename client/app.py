"""
Flask web UI for the Distributed File System client.
Run from project root: python -m client.app
"""

import grpc
from io import BytesIO
from flask import Flask, render_template, request, jsonify, send_file

from common.config import CHUNK_SIZE
from proto import master_pb2, master_pb2_grpc, chunkserver_pb2, chunkserver_pb2_grpc
from proto.master_pb2_grpc import MasterServiceStub

app = Flask(__name__)

MASTER_ADDRESS = "localhost:5050"


def translateAddress(dockerAddress: str) -> str:
    port = dockerAddress.split(":")[1]
    return f"localhost:{port}"


def getMasterStub():
    channel = grpc.insecure_channel(MASTER_ADDRESS)
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
    return render_template("index.html")


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

    dataBytes = file.read()
    size = len(dataBytes)

    try:
        masterStub = getMasterStub()
        response = masterStub.UploadFile(
            master_pb2.UploadFileRequest(filename=name, file_size=size)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    if not response.success:
        return jsonify({"error": response.message}), 400

    for assignment in response.assignments:
        chunkIndex = assignment.chunk_index
        chunkData = dataBytes[chunkIndex * CHUNK_SIZE: (chunkIndex + 1) * CHUNK_SIZE]

        addresses = assignment.server_addresses[:]
        primaryAddress = translateAddress(addresses[0])
        forwardAddresses = addresses[1:]

        chunkChannel = grpc.insecure_channel(primaryAddress)
        chunkStub = chunkserver_pb2_grpc.ChunkServerServiceStub(chunkChannel)

        STREAM_SIZE = 512 * 1024

        def makeIterator(chunk, chunkHandle, forwards):
            for i in range(0, len(chunk), STREAM_SIZE):
                yield chunkserver_pb2.WriteChunkRequest(
                    chunk_handle=chunkHandle,
                    data=chunk[i:i + STREAM_SIZE],
                    forward_addresses=forwards,
                )

        try:
            writeResponse = chunkStub.WriteChunk(
                makeIterator(chunkData, assignment.chunk_handle, forwardAddresses)
            )
            if not writeResponse.success:
                return jsonify({"error": f"Chunk {chunkIndex} write failed: {writeResponse.message}"}), 500
        except grpc.RpcError as e:
            return jsonify({"error": f"Chunk {chunkIndex} write error: {e.details()}"}), 500

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
        chunkChannel = grpc.insecure_channel(address)
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
        response = stub.SearchFiles(
            master_pb2.SearchFilesRequest(query=query)
        )
    except grpc.RpcError as e:
        return jsonify({"error": e.details()}), 500

    results = []
    for r in response.results:
        results.append({
            "filename": r.filename,
            "score": round(r.score, 4),
            "line_number": r.line_number,
            "snippet": r.snippet,
        })

    return jsonify(results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
