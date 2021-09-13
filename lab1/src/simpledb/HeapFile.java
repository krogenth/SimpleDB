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
	File file;
	TupleDesc td;
	List<HeapPage> pages = new ArrayList<HeapPage>();
	int id;

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
    	
    	Database.getCatalog().addTable(this, String.valueOf(this.id));
    	
    	HeapPageId hpid = new HeapPageId(this.id, this.pages.size());
    	try {
			this.pages.add(new HeapPage(hpid, Files.readAllBytes(this.file.toPath())));
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
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
    	return id;
        //throw new UnsupportedOperationException("implement this");
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	return td;
    	//throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	for (HeapPage var : this.pages) {
    		if (var.getId().equals(pid))
    			return var;
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
        return this.pages.size();
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
    	
    	class PageIterator implements DbFileIterator {
    		int pageIndex = 0;
    		List<HeapPage> pages;
    		Iterator<Tuple> tupleIterator;
    		
    		public void addPages(List<HeapPage> pageList) {
    			this.pages = pageList;
    			
    		}
    		
    		public void open()
    			throws DbException, TransactionAbortedException {
    			if (this.pages != null && this.pages.size() > 0) {
    				tupleIterator = this.pages.get(this.pageIndex).iterator();
    			}
    		}
    		
    		public boolean hasNext()
    			throws DbException, TransactionAbortedException {
    			if (this.tupleIterator != null) {
	    			if (this.tupleIterator.hasNext())
	    				return true;
	    			else {
	    				if (++this.pageIndex < this.pages.size()) {
	    					this.tupleIterator = this.pages.get(this.pageIndex).iterator();
	    					if (this.tupleIterator.hasNext())
	    						return true;
	    					else
	    						return false;
	    				}
	    				else
	    					return false;
	    			}
    			}
    			else
    				return false;
    		}
    		
    		public Tuple next()
    			throws DbException, TransactionAbortedException, NoSuchElementException {
    			if (this.hasNext())
    				return this.tupleIterator.next();
    			else
    				throw new NoSuchElementException();
    		}
    		
    		public void rewind()
    			throws DbException, TransactionAbortedException {
    			this.pageIndex = 0;
    			if (this.tupleIterator != null)
    				this.tupleIterator = this.pages.get(this.pageIndex).iterator();
    		}
    		
    		public void close() {
    			this.tupleIterator = null;
    		}
    	}
    	
    	PageIterator it = new PageIterator();
    	it.addPages(this.pages);
    	
        return it;
    }
    
}

