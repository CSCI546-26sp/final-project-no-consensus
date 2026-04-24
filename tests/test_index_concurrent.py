import threading                                                                                                                                         
                                                                                
from chunkserver.index import InvertedIndex                                                                                                              

                                                                                                                                                        
def testTokenizeChunkIsPure():                                                   
    idx = InvertedIndex()
    postings, lineBreaks = idx.tokenizeChunk("c1", "hello world\nhello again")
    assert idx.totalChunks == 0                                                                                                                          
    assert "c1" not in idx.chunkTerms
    assert "c1" not in idx.lineBreaks                                                                                                                    
    assert "hello" in postings and "world" in postings                                                                                                   
    print("PASS: tokenizeChunk does not mutate shared state")
                                                                                                                                                        
                                                                                
def testTokenizeMergeEquivalentToIndexChunk():                                                                                                           
    a = InvertedIndex()                                                          
    b = InvertedIndex()
    text = "hello world this is a test of the indexing system\nsecond line here"
    a.indexChunk("h1", text)                                                                                                                             
    postings, lineBreaks = b.tokenizeChunk("h1", text)
    b.mergeChunk("h1", postings, lineBreaks)                                                                                                             
    assert a.totalChunks == b.totalChunks                                                                                                                
    assert set(a.index.keys()) == set(b.index.keys())
    assert a.chunkTerms["h1"] == b.chunkTerms["h1"]                                                                                                      
    assert list(a.lineBreaks["h1"]) == list(b.lineBreaks["h1"])                                                                                          
    for term in a.index:
        ap = a.index[term][0]                                                                                                                            
        bp = b.index[term][0]                                                    
        assert ap.frequency == bp.frequency                                                                                                              
        assert ap.positions == bp.positions                                      
    print("PASS: tokenize+merge equivalent to indexChunk")                                                                                               
                                                                                                                                                        

def testConcurrentMergesNoLostUpdates():                                                                                                                 
    """16 threads tokenize independently, then merge under a shared lock —       
    simulates the chunkserver's RWLock-protected merge."""                                                                                               
    idx = InvertedIndex()                                                                                                                                
    lock = threading.Lock()                                                                                                                              
    N = 16                                                                                                                                               
                                                                                
    def worker(i):
        text = f"alpha beta gamma delta unique{i} chunk{i}"
        postings, lineBreaks = idx.tokenizeChunk(f"c{i}", text)                                                                                          
        with lock:
            idx.mergeChunk(f"c{i}", postings, lineBreaks)                                                                                                
                                                                                                                                                        
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in threads: t.start()                                                                                                                          
    for t in threads: t.join()                                                                                                                           

    assert idx.totalChunks == N, f"Expected {N} chunks, got {idx.totalChunks}"                                                                           
    for i in range(N):                                                           
        postings = idx.index.get(f"unique{i}")                                                                                                           
        assert postings is not None and len(postings) == 1, f"unique{i} should have 1 posting, has {len(postings) if postings else 0}"
    for term in ("alpha", "beta", "gamma", "delta"):                                                                                                     
        assert len(idx.index[term]) == N, \
            f"{term} should have {N} postings, has {len(idx.index[term])}"                                                                               
    print("PASS: concurrent merges have no lost updates")                        
                                                                                                                                                        
                                                                                                                                                        
def testConcurrentTokenizeWhileMerging():
    """One thread merges; others tokenize a different chunk concurrently.                                                                                
    Tokenize must not see partial merge state — it never reads shared state at all."""                                                                   
    idx = InvertedIndex()                                                                                                                                
    lock = threading.Lock()                                                                                                                              
                                                                                                                                                        
    # seed with a chunk so the index has shared state to read from                                                                                       
    idx.indexChunk("seed", "alpha beta gamma " * 100)
                                                                                                                                                        
    errors = []                                                                  
    def merger():                                                                                                                                        
        for i in range(50):                                                      
            postings, lineBreaks = idx.tokenizeChunk(f"m{i}", f"alpha beta gamma item{i}")
            with lock:                                                                                                                                   
                idx.mergeChunk(f"m{i}", postings, lineBreaks)
                                                                                                                                                        
    def tokenizer():                                                             
        try:
            for i in range(50):                                                                                                                          
                postings, lineBreaks = idx.tokenizeChunk(f"t{i}", f"delta epsilon zeta item{i}")
                assert "delta" in postings and f"item{i}" in postings                                                                                    
        except Exception as ex:                                                                                                                          
            errors.append(ex)                                                                                                                            
                                                                                                                                                        
    threads = [threading.Thread(target=merger), threading.Thread(target=tokenizer),                                                                      
                threading.Thread(target=tokenizer)]
    for t in threads: t.start()                                                                                                                          
    for t in threads: t.join()                                                   
    assert not errors, f"Tokenize raised: {errors}"
    print("PASS: tokenize runs cleanly alongside concurrent merges")                                                                                     
                                                                                                                                                        
                                                                                                                                                        
if __name__ == "__main__":                                                                                                                               
    testTokenizeChunkIsPure()                                                    
    testTokenizeMergeEquivalentToIndexChunk()
    testConcurrentMergesNoLostUpdates()                                                                                                                  
    testConcurrentTokenizeWhileMerging()