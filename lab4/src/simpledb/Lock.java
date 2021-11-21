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
    private long maxTime = 100;
    private TimeUnit maxTimeUnit = TimeUnit.MILLISECONDS;
	
	public enum LockMode {
        SHARED,
        EXCLUSIVE
    }
	
	Lock() {
		
	}
	
	public void acquire(TransactionId tid, LockMode mode) throws InterruptedException {
		if (mode == LockMode.SHARED) {
			this.acquireSharedLock(tid);
		} else if (mode == LockMode.EXCLUSIVE) {
			this.acquireExclusiveLock(tid);
		} else {
			throw new RuntimeException("Invalid LockMode");
		}
	}
	
	public void release(TransactionId tid) {
		this.lock.lock();
		try {
			if (this.getLockMode() == LockMode.SHARED && this.sharedLockCount > 0) {
				this.sharedLockCount--;
				if (this.sharedLockCount == 0)
					this.mode = null;
			} else {
				this.mode = null;
			}
			this.owners.remove(tid);
			this.waiters.signalAll();
		} finally {
			this.lock.unlock();
		}
	}
	
	public void upgradeLock(TransactionId tid) throws InterruptedException {
		this.lock.lock();
		
		if (this.owners.contains(tid) && this.getLockMode() == LockMode.EXCLUSIVE)
			return;
			
		while (this.getLockMode() == LockMode.EXCLUSIVE || this.sharedLockCount > 1) {
			if (!waiters.await(this.maxTime, this.maxTimeUnit)) {
				this.lock.unlock();
				throw new InterruptedException();
			}
		}
		this.sharedLockCount = 0;
		this.mode = LockMode.EXCLUSIVE;
		this.lock.unlock();
	}
	
	public LockMode getLockMode() {
		return this.mode;
	}
	
	private void acquireSharedLock(TransactionId tid) throws InterruptedException {
		this.lock.lock();
		
		while (this.getLockMode() == LockMode.EXCLUSIVE || this.lock.hasWaiters(waiters)) {
			if (!waiters.await(this.maxTime, this.maxTimeUnit)) {
				lock.unlock();
				throw new InterruptedException();
			}
		}
		this.sharedLockCount++;
		this.owners.add(tid);
		this.mode = LockMode.SHARED;
		this.lock.unlock();
	}
	
	private void acquireExclusiveLock(TransactionId tid) throws InterruptedException {
		this.lock.lock();
		
		while (this.getLockMode() == LockMode.EXCLUSIVE || this.getLockMode() == LockMode.SHARED) {
			if (!waiters.await(this.maxTime, this.maxTimeUnit)) {
				this.lock.unlock();
				throw new InterruptedException();
			}
		}
		this.owners.add(tid);
		this.mode = LockMode.EXCLUSIVE;
		this.lock.unlock();
	}

}
