import sys
import os

# Add proto directory to sys.path so generated imports like
# "import master_pb2" resolve correctly
sys.path.insert(0, os.path.dirname(__file__))
