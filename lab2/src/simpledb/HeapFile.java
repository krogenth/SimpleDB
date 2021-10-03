package simpledb;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;
	private int id;
	private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.td = td;
    	this.id = f.getAbsoluteFile().hashCode();
    	this.numPages = (int) f.length() / BufferPool.PAGE_SIZE;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        // some code goes here
    	return this.id;
        //throw new UnsupportedOperationException("implement this");
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	return this.td;
    	//throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pageSize = Database.getBufferPool().PAGE_SIZE;
    	int pageOffset = pid.pageno() * pageSize;
    	byte[] bytes = new byte[pageSize];
    	int bytesRead = 0;
    	
    	try {
            // add a new blank page to the HeapFile
            if (pid.pageno() == this.numPages()) {
                this.numPages++;
                return new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            // read the existing page from disk
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
                randomAccessFile.seek(pageOffset);
                randomAccessFile.read(bytes);
                randomAccessFile.close();
                return new HeapPage((HeapPageId) pid, bytes);
            }
    	}
    	catch (FileNotFoundException e) {
    		e.printStackTrace();
    	}
    	catch (IOException e) {
    		e.printStackTrace();
    	}
    	
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	PageId pid = page.getId();
        long pageOffset = pid.pageno() * BufferPool.PAGE_SIZE;
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        randomAccessFile.seek(pageOffset);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	ArrayList<Page> modifiedPages = new ArrayList<>();
        HeapPage pageWithSpace = null;

        // find a page that has an space for a new tuple
        for (int currentPageNo = 0; currentPageNo < this.numPages(); currentPageNo++) {
            HeapPageId pageId = new HeapPageId(this.getId(), currentPageNo);
            HeapPage currentPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (currentPage.getNumEmptySlots() > 0) {
                pageWithSpace = currentPage;
                break;
            }
        }

        // add tuple to a page with space
        if (pageWithSpace != null) {
            pageWithSpace.addTuple(t);
            modifiedPages.add(pageWithSpace);
        //  create a new blank page and add the tuple
        } else {
            HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage newPage = (HeapPage)Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
            newPage.addTuple(t);
            modifiedPages.add(newPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        HeapPage pageToDeleteFrom = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        pageToDeleteFrom.deleteTuple(t);

        return pageToDeleteFrom;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	class HeapFileIterator extends AbstractDbFileIterator {
    	    private TransactionId tid = null;
    	    private HeapFile table = null;
    	    private int nextPageNo = 0;
    	    private Iterator<Tuple> pageIterator = null;
    	    		
    	    public HeapFileIterator(HeapFile table, TransactionId tid) {
    	    	this.table = table;
    	    	this.tid = tid;
    	    }
    	    		
    	    public void open()
    	    	throws DbException, TransactionAbortedException {
    	    	this.pageIterator = this.getNextPageIterator();
    	    }
    	    		
    	    public void close() {
    	    	super.close();
    	    	this.pageIterator = null;
    	    	this.nextPageNo = 0;
    	    }
    	    		
    	    public void rewind()
    	        throws DbException, TransactionAbortedException {
    	        this.close();
    	        this.open();
    	    }
    	    		
    	    public Tuple readNext()
    	    	throws DbException, TransactionAbortedException, NoSuchElementException {
    	    	if (this.pageIterator == null)
    	    		return null;
    	    			
    	    	if (this.pageIterator.hasNext())
    	    		return this.pageIterator.next();
    	    	else if (this.nextPageNo < this.table.numPages()) {
    	    		this.pageIterator = this.getNextPageIterator();
    	    		if (this.pageIterator.hasNext())
    	    			return this.pageIterator.next();
    	    	}
    	    	
    	    	return null;
    	    }
    	    		
    	    private Iterator<Tuple> getNextPageIterator()
    	    	throws DbException, TransactionAbortedException {
    	    	return this.retrieveNextPage().iterator();
    	    }
    	    		
    		private HeapPage retrieveNextPage() 
    			throws TransactionAbortedException, DbException {
    			HeapPageId nextPid = new HeapPageId(this.table.getId(), this.nextPageNo);
    	    	this.nextPageNo++;
    	    	return (HeapPage)Database.getBufferPool().getPage(this.tid,  nextPid, Permissions.READ_ONLY);
    		}
    	}
    	
        return new HeapFileIterator(this, tid);
    }
    
}

