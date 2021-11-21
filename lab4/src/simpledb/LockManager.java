package simpledb;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {	
	private ConcurrentHashMap<PageId, Lock> lockTable = null;
	private ConcurrentHashMap<TransactionId, TransactionId> tidTable = null;
	
	public LockManager() {
		this.lockTable = new ConcurrentHashMap<>();
		this.tidTable = new ConcurrentHashMap<>();
	}
	
	public void lock(PageId pid, TransactionId tid, Lock.LockMode mode) throws DeadlockException {
		if (tid == null || pid == null)
			return;
		
		this.lockTable.putIfAbsent(pid, new Lock());
		Lock lock = this.lockTable.get(pid);
		
		try {
			if(this.hasLock(pid, tid) && mode == lock.getLockMode()) {
				return;
			} else if (mode == Lock.LockMode.SHARED && this.hasLock(pid, tid) && lock.getLockMode() == Lock.LockMode.EXCLUSIVE) {
				return;
			} else if (mode == Lock.LockMode.EXCLUSIVE && this.hasLock(pid,  tid) && lock.getLockMode() == Lock.LockMode.SHARED) {
				lock.upgradeLock(tid);
			} else {
				lock.acquire(tid, mode);
			}
		} catch (InterruptedException e) {
			this.removeTransaction(tid);
			throw new DeadlockException(e);
		}
		
		tid.addLock(lock);
		this.tidTable.putIfAbsent(tid, tid);
	}
	
	public void unlock(PageId pid, TransactionId tid) {
		if (tid == null || pid == null)
			return;
		
		Lock lock = lockTable.get(pid);
		if (lock != null) {
			lock.release(tid);
		}
		
		tid.removeLock(lock);
	}
	
	public void removeTransaction(TransactionId tid) {
		if (tid == null || !this.tidTable.contains(tid))
			return;
		
		Set<Lock> locks = tid.getLocks();
		
		for(Lock lock : locks) {
			lock.release(tid);
		}
		
		this.tidTable.remove(tid);
	}
	
	public boolean hasLock(PageId pid, TransactionId tid) {
		if (tid == null || !this.tidTable.contains(tid))
			return false;
		
		Set<Lock> locks = tid.getLocks();
		if (locks == null)
			return false;
		
		for (Lock lock : locks) {
			if (lock == this.lockTable.get(pid))
				return true;
		}
		
		return false;
	}
}
