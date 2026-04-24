import os
import shutil

class ChunkStore:
    def __init__(self, dataDir):
        self.dataDir = dataDir
        os.makedirs(dataDir, exist_ok = True)
        return
    
    def writeChunk(self, chunkHandle, data):
        path = self.getChunkPath(chunkHandle)
        with open(path, "wb") as f:
            f.write(data)

    def readChunk(self, chunkHandle):
        path = self.getChunkPath(chunkHandle)
        if not os.path.exists(path): 
            raise FileNotFoundError(f"Specified chunkhandle {chunkHandle} does not exist")
        with open(path, 'rb') as f:
            return f.read()
        
    def deleteChunk(self, chunkHandle):
        path = self.getChunkPath(chunkHandle)
        if os.path.exists(path): 
            os.remove(path)
        return
    
    def listChunks(self):
        return [f for f in os.listdir(self.dataDir) if os.path.isfile(os.path.join(self.dataDir, f))]
    
    def getUsedBytes(self):
        return sum(
            os.path.getsize(os.path.join(self.dataDir, f))
            for f in os.listdir(self.dataDir)
            if os.path.isfile(os.path.join(self.dataDir, f))
        )

    def getAvailableDisk(self):
        return shutil.disk_usage(self.dataDir).free - self.getUsedBytes()
    
    def chunkExists(self, chunkHandle):
        return os.path.exists(self.getChunkPath(chunkHandle))
    
    def getChunkPath(self, chunkHandle):
        return os.path.join(self.dataDir, chunkHandle)
