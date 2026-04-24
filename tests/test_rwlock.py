import threading                                                                                                                                         
import time                                                                     

from chunkserver.rwlock import RWLock

                                                                                                                                                        
def testReadersConcurrent():
    lock = RWLock()                                                                                                                                      
    inside = 0                                                                  
    peak = 0
    cond = threading.Condition()
    release = threading.Event()                                                                                                                          

    def reader():                                                                                                                                        
        nonlocal inside, peak                                                   
        with lock.readLock():
            with cond:                                                                                                                                   
                inside += 1
                peak = max(peak, inside)                                                                                                                 
                cond.notify_all()                                               
            release.wait()
            with cond:                                                                                                                                   
                inside -= 1
                                                                                                                                                        
    threads = [threading.Thread(target=reader) for _ in range(5)]               
    for t in threads:
        t.start()                                                                                                                                        
    # wait until all five are inside simultaneously
    deadline = time.time() + 2.0                                                                                                                         
    with cond:                                                                  
        while peak < 5 and time.time() < deadline:                                                                                                       
            cond.wait(timeout=0.1)                                                                                                                       
    assert peak == 5, f"Expected 5 concurrent readers, peaked at {peak}"
    release.set()                                                                                                                                        
    for t in threads:                                                           
        t.join()                                                                                                                                         
    print("PASS: readers run concurrently")                                     


def testWriterExcludesReaders():
    lock = RWLock()
    events = []                                                                                                                                          
    writerHolding = threading.Event()
    writerRelease = threading.Event()                                                                                                                    
                                                                                
    def writer():                                                                                                                                        
        with lock.writeLock():                                                  
            events.append("writer-in")
            writerHolding.set()                                                                                                                          
            writerRelease.wait()
            events.append("writer-out")                                                                                                                  
                                                                                
    def reader():
        with lock.readLock():
            events.append("reader-in")
                                                                                                                                                        
    wt = threading.Thread(target=writer)
    wt.start()                                                                                                                                           
    writerHolding.wait()                                                        

    rt = threading.Thread(target=reader)                                                                                                                 
    rt.start()
    # reader should be blocked while writer holds the lock                                                                                               
    time.sleep(0.1)                                                                                                                                      
    assert "reader-in" not in events, f"Reader entered while writer held lock: {events}"
                                                                                                                                                        
    writerRelease.set()                                                         
    wt.join()                                                                                                                                            
    rt.join()                                                                   
    assert events == ["writer-in", "writer-out", "reader-in"], events
    print("PASS: writer excludes readers")                                                                                                               

                                                                                                                                                        
def testWriterPreferredOverNewReaders():                                        
    lock = RWLock()                                                                                                                                      
    order = []                                                                  
    firstReaderIn = threading.Event()
    firstReaderRelease = threading.Event()                                                                                                               
    writerReady = threading.Event()
                                                                                                                                                        
    def firstReader():                                                          
        with lock.readLock():
            order.append("r1-in")                                                                                                                        
            firstReaderIn.set()
            firstReaderRelease.wait()                                                                                                                    
            order.append("r1-out")                                              
                                                                                                                                                        
    def writer():
        writerReady.set()                                                                                                                                
        with lock.writeLock():                                                  
            order.append("w-in")
            order.append("w-out")
                                                                                                                                                        
    def lateReader():
        with lock.readLock():                                                                                                                            
            order.append("r2-in")                                               

    r1 = threading.Thread(target=firstReader)                                                                                                            
    r1.start()
    firstReaderIn.wait()                                                                                                                                 
                                                                                
    w = threading.Thread(target=writer)
    w.start()
    writerReady.wait()                                                                                                                                   
    # give the writer time to enter the waiting queue
    time.sleep(0.05)                                                                                                                                     
                                                                                
    r2 = threading.Thread(target=lateReader)                                                                                                             
    r2.start()                                                                  
    # r2 should be blocked because a writer is waiting
    time.sleep(0.1)                                                                                                                                      
    assert "r2-in" not in order, f"Late reader jumped ahead of waiting writer: {order}"
                                                                                                                                                        
    firstReaderRelease.set()                                                    
    r1.join()                                                                                                                                            
    w.join()                                                                    
    r2.join()
    # writer must run before late reader
    assert order.index("w-in") < order.index("r2-in"), order                                                                                             
    print("PASS: waiting writer blocks new readers")                                                                                                     
                                                                                                                                                        
                                                                                                                                                        
if __name__ == "__main__":                                                      
    testReadersConcurrent()
    testWriterExcludesReaders()                                                                                                                          
    testWriterPreferredOverNewReaders()