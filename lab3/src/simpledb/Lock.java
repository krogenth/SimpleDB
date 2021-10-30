package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

public class Lock {
	private final Set<TransactionId> owners = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition waiters = lock.newCondition();
    private int sharedLockCount = 0;
    private LockMode mode = null;
    private long maxTime = 500;
    private TimeUnit maxTimeUnit = TimeUnit.MILLISECONDS;
	
	public enum LockMode {
        SHARED,
        EXCLUSIVE
    }
	
	Lock() {
		
	}
	
	void acquire(TransactionId tid, LockMode mode) throws InterruptedException {
		if (mode == LockMode.SHARED) {
			this.acquireSharedLock(tid);
		} else if (mode == LockMode.EXCLUSIVE) {
			this.acquireExclusiveLock(tid);
		}
	}
	
	void release(TransactionId tid) {
		lock.lock();
		try {
			if (this.getLockMode() == LockMode.SHARED && this.sharedLockCount > 0) {
				this.sharedLockCount--;
				if (this.sharedLockCount == 0)
					this.mode = null;
			} else {
				this.mode = null;
			}
			owners.remove(tid);
			waiters.signalAll();
		} finally {
			lock.unlock();
		}
	}
	
	void upgradeLock(TransactionId tid) throws InterruptedException {
		lock.lock();
		try {
			if (this.owners.contains(tid) && this.getLockMode() == LockMode.EXCLUSIVE) {
				return;
			}
			while (this.getLockMode() == LockMode.EXCLUSIVE) {
				waiters.await();
			}
			this.sharedLockCount = 0;
			this.mode = LockMode.EXCLUSIVE;
			
		} finally {
			lock.unlock();
		}
	}
	
	LockMode getLockMode() {
		return this.mode;
	}
	
	void acquireSharedLock(TransactionId tid) throws InterruptedException {
		lock.lock();
        try {
            while (this.getLockMode() == LockMode.EXCLUSIVE || lock.hasWaiters(waiters)) {
                waiters.await();
            }
            this.sharedLockCount++;
            owners.add(tid);
            this.mode = LockMode.SHARED;
        } finally {
            lock.unlock();
        }
	}
	
	void acquireExclusiveLock(TransactionId tid) throws InterruptedException {
		lock.lock();
        try {
            while (this.getLockMode() == LockMode.EXCLUSIVE || this.getLockMode() == LockMode.SHARED) {
                if (!waiters.await(this.maxTime, this.maxTimeUnit)) {
                	lock.unlock();
                	Thread.currentThread().interrupt();
                }
            }
            owners.add(tid);
            this.mode = LockMode.EXCLUSIVE;
        } finally {
            lock.unlock();
        }
	}

}
