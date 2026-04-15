import os
import shutil

class ChunkStore:
    def __init__(self, dataDir):
        self.dataDir = dataDir
        os.makedirs(dataDir, exist_ok = True)
        return
    
    def writeChunk(self, chunkHandle, data):
        path = os.path.join(self.dataDir, chunkHandle)
        with open(path, "wb") as f:
            f.write(data)

    def readChunk(self, chunkHandle):
        path = os.path.join(self.dataDir, chunkHandle)
        if not os.path.exists(path): 
            raise FileNotFoundError(f"Specified chunkhandle {chunkHandle} does not exist")
        with open(path, 'rb') as f:
            return f.read()
        
    def deleteChunk(self, chunkHandle):
        path = os.path.join(self.dataDir, chunkHandle)
        if os.path.exists(path): 
            os.remove(path)
        return
    
    def listChunks(self):
        return [f for f in os.listdir(self.dataDir) if os.path.isfile(os.path.join(self.dataDir, f))]
    
    def getAvailableDisk(self):
        return shutil.disk_usage(self.dataDir).free
    
    def chunkExists(self, chunkHandle):
        return os.path.exists(os.path.join(self.dataDir, chunkHandle))
