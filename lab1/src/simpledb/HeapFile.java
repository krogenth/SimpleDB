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
    		RandomAccessFile accessFile = new RandomAccessFile(this.file, "r");
        	accessFile.seek(pageOffset);
    		bytesRead = accessFile.read(bytes);
    		accessFile.close();
    		
    		if (bytesRead < pageSize) {
        		throw new IllegalArgumentException();
        	}
        	
        	return new HeapPage((HeapPageId)pid, bytes);
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.file.length() / Database.getBufferPool().PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	
    	class HeapFileIterator implements DbFileIterator {
    		private TransactionId tid = null;
    		private HeapFile table = null;
    		private HeapPage page = null;
    		private Iterator<Tuple> pageIterator = null;
    		
    		public HeapFileIterator(HeapFile table, TransactionId tid) {
    			this.table = table;
    			this.tid = tid;
    		}
    		
    		public void open()
    			throws DbException, TransactionAbortedException {
    			this.page = (HeapPage)Database.getBufferPool().getPage(this.tid, new HeapPageId(this.table.getId(), 0), Permissions.READ_ONLY);
    			this.pageIterator = this.page.iterator();
    		}
    		
    		public void close() {
    			this.page = null;
    			this.pageIterator = null;
    		}
    		
    		public void rewind()
        		throws DbException, TransactionAbortedException {
        		this.close();
        		this.open();
        	}
    		
    		public boolean hasNext()
    			throws DbException, TransactionAbortedException {
    			if (this.pageIterator == null)
    				return false;
    			
    			if (this.pageIterator.hasNext())
    				return true;
    			else
    				return this.hasNextPage();
    		}
    		
    		public Tuple next()
    			throws DbException, TransactionAbortedException, NoSuchElementException {
    			if (this.pageIterator == null)
    				throw new NoSuchElementException();
    			
    			if (this.pageIterator.hasNext())
    				return this.pageIterator.next();
    			else if (this.hasNextPage()) {
    				this.page = this.retrieveNextPage();
    				this.pageIterator = this.page.iterator();
    				return this.pageIterator.next();
    			}
    			else
    				throw new NoSuchElementException();
    		}
    		
    		private boolean hasNextPage() {
    			return this.page.getId().pageno() < (this.table.numPages() - 1);
    		}
    		
    		private HeapPage retrieveNextPage() 
    			throws TransactionAbortedException, DbException {
    			HeapPageId nextPid = new HeapPageId(this.table.getId(), this.page.getId().pageno() + 1);
    			return (HeapPage)Database.getBufferPool().getPage(this.tid,  nextPid, Permissions.READ_ONLY);
    		}
    	}
        return new HeapFileIterator(this, tid);
    }
    
}

