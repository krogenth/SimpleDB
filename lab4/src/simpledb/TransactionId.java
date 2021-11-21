package simpledb;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId {
    static AtomicLong counter = new AtomicLong(0);
    long myid;
    private Set<Lock> locks = new HashSet<>();
    
    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object tid) {
    	if (tid == null)
        	return false;
        if (this == tid)
        	return true;
        if (!(tid instanceof TransactionId))
        	return false;
        return this.myid == ((TransactionId)tid).myid;
    }

    public int hashCode() {
        return (int) myid;
    }
    
    void addLock(Lock lock) {
    	this.locks.add(lock);
    }
    
    void removeLock(Lock lock) {
    	this.locks.remove(lock);
    }
    
    Set<Lock> getLocks() {
    	return locks;
    }
}
