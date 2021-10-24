package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private Map<PageId, Page> pages = null;
    private Map<TransactionId, Set<PageId>> transactionMap = null;
    private int maxPages = 0;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.pages = new ConcurrentHashMap<PageId, Page>();
    	this.transactionMap = new ConcurrentHashMap<TransactionId, Set<PageId>>();
    	this.maxPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	try {
    		Database.getLockManager().lock(pid,  tid, perm.adjustForLock());
    	} catch (LockedException e) {
    		throw new TransactionAbortedException();
    	}
    	
    	if (tid != null ) {
	    	Set<PageId> transactionPages = transactionMap.getOrDefault(tid, new HashSet<>());
	    	transactionPages.add(pid);
	    	transactionMap.put(tid, transactionPages);
    	}
    	
    	if (this.pages.containsKey(pid)) {
    		Page page = this.pages.get(pid);
    		page.updateAccessTimestamp();
    		return page;
    	}
    	
    	if (this.pages.size() >= this.maxPages)
    		this.evictPage();
    	
    	Page page = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
    	this.pages.put(pid,  page);
    	
    	return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	Database.getLockManager().unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Set<PageId> transactionPages = transactionMap.getOrDefault(tid, Collections.emptySet());
    	for (PageId p: transactionPages) {
    		this.flushPage(p);
    	}
    	
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return Database.getLockManager().hasLock(p,  tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Set<PageId> tidPids = this.transactionMap.getOrDefault(tid, Collections.emptySet());
    	for (PageId pid : tidPids) {
    		if (commit) {
    			this.flushPage(pid);
    		} else {
    			if (this.pages.get(pid) != null && tid.equals(this.pages.get(pid).isDirty())) {
    				Page restoredPage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
    				this.pages.put(pid,  restoredPage);
    			}
    		}
    	}
    	
    	Database.getLockManager().removeTransaction(tid);
    	this.transactionMap.remove(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile file = Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> dirtyPages = file.addTuple(tid,  t);
    	for (Page dirtyPage: dirtyPages)
    		dirtyPage.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
    	Page dirtyPage = file.deleteTuple(tid,  t);
    	dirtyPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pid: this.pages.keySet())
    		this.flushPage(pid);

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page page = this.pages.get(pid);
    	
    	if (page != null && page.isDirty() != null) {
    		DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
    		file.writePage(page);
    		page.markDirty(false, null);
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }
    
    private Page findLRUPage() {
    	Page LRUPage = null;
    	
    	for (Page page: this.pages.values()) {
    		if (page.isDirty() != null)
    			continue;
    		else if (LRUPage == null || page.getAccessTimestamp() < LRUPage.getAccessTimestamp())
    			LRUPage = page;
    	}
    	
    	return LRUPage;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Page LRUPage = this.findLRUPage();
    	if (LRUPage == null)
    		throw new DbException("No pages to evict!");
    	
    	try {
    		this.flushPage(LRUPage.getId());
    		this.pages.remove(LRUPage.getId());
    	} catch (IOException e) {
    		throw new DbException("Could not remove page");
    	}
    }

}
