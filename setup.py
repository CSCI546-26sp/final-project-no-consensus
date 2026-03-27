import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Directories to create
dirs = [
    "master",
    "chunkserver",
    "client",
    "common",
    "docker",
    "scripts",
    "tests",
    "proto",
]

# Empty files to create
empty_files = [
    "master/__init__.py",
    "chunkserver/__init__.py",
    "client/__init__.py",
    "common/__init__.py",
    "tests/__init__.py",
    "master/server.py",
    "chunkserver/server.py",
    "client/cli.py",
    "common/config.py",
    "common/utils.py",
    "docker/Dockerfile",
    "docker/docker-compose.yml",
    "scripts/run_workload.py",
]

# Files with content
files_with_content = {
    "requirements.txt": (
        "grpcio==1.68.1\n"
        "grpcio-tools==1.68.1\n"
        "protobuf==5.29.2\n"
        "click==8.1.7\n"
    ),
    ".gitignore": (
        "__pycache__/\n"
        "*.pyc\n"
        "*.pyo\n"
        ".env\n"
        "venv/\n"
        "data/\n"
        "*.egg-info/\n"
        "dist/\n"
        "build/\n"
    ),
}

# Create directories
for d in dirs:
    path = os.path.join(BASE_DIR, d)
    os.makedirs(path, exist_ok=True)

# Create empty files
for f in empty_files:
    path = os.path.join(BASE_DIR, f)
    if not os.path.exists(path):
        open(path, "w").close()

# Create files with content
for f, content in files_with_content.items():
    path = os.path.join(BASE_DIR, f)
    if not os.path.exists(path):
        with open(path, "w") as fh:
            fh.write(content)

print("Project structure created successfully.")
