package simpledb;

import java.util.Arrays;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    private int[] minPerField = null;
    private int[] maxPerField = null;
    private int ioCostPerPage = 0;
    private IntHistogram[] intHistograms = null;
    private StringHistogram[] stringHistograms = null;
    private int tableid = 0;
    private TupleDesc td = null;
    private int numTuples = 0;
    
    
	
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
    	this.ioCostPerPage = ioCostPerPage;
    	this.tableid = tableid;
    	this.td = Database.getCatalog().getTupleDesc(tableid);
    	int numTupleFields = this.td.numFields();
    	this.intHistograms = new IntHistogram[numTupleFields];
    	this.stringHistograms = new StringHistogram[numTupleFields];
    	this.minPerField = new int[numTupleFields];
    	this.maxPerField = new int[numTupleFields];
    	Arrays.fill(this.minPerField, Integer.MAX_VALUE);
    	Arrays.fill(this.maxPerField, Integer.MIN_VALUE);
    	
    	try {
    		this.getMinMaxPerTupleField();
    		this.getHistograms();
    		this.countTuples();
    	} catch(DbException e) {
    		e.printStackTrace();
    	} catch(TransactionAbortedException e) {
    		e.printStackTrace();
    	}
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	// some code goes here
        return this.ioCostPerPage * ((HeapFile)Database.getCatalog().getDbFile(this.tableid)).numPages();
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	// some code goes here
        return (int)(this.numTuples * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	// some code goes here
        if(this.td.getType(field) == Type.INT_TYPE) {
        	return this.intHistograms[field].estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (this.td.getType(field) == Type.STRING_TYPE) {
        	return this.stringHistograms[field].estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
        	return 0.0;
        }
    }
    
    private void getMinMaxPerTupleField() throws DbException, TransactionAbortedException {
    	DbFileIterator it = Database.getCatalog().getDbFile(this.tableid).iterator(new TransactionId());
    	it.open();
    	
    	while(it.hasNext()) {
    		Tuple t = it.next();
    		for(int fieldIndex = 0; fieldIndex < this.td.numFields(); fieldIndex++) {
    			if (td.getType(fieldIndex) == Type.INT_TYPE) {
    				if(((IntField)t.getField(fieldIndex)).getValue() < this.minPerField[fieldIndex]) {
    					this.minPerField[fieldIndex] = ((IntField)t.getField(fieldIndex)).getValue();
    				}
    				if(((IntField)t.getField(fieldIndex)).getValue() > this.maxPerField[fieldIndex]) {
    					this.maxPerField[fieldIndex] = ((IntField)t.getField(fieldIndex)).getValue();
    				}
    			}
    		}
    	}
    	
    	it.close();
    }
    
    private void getHistograms() throws DbException, TransactionAbortedException {
    	DbFileIterator it = Database.getCatalog().getDbFile(this.tableid).iterator(new TransactionId());
    	it.open();
    	
    	while (it.hasNext()) {
    		Tuple t = it.next();
    		
    		for(int fieldIndex = 0; fieldIndex < this.td.numFields(); fieldIndex++) {
    			Field field = t.getField(fieldIndex);
    			if(this.td.getType(fieldIndex) == Type.INT_TYPE) {
	    			if(this.intHistograms[fieldIndex] == null) {
	    				this.intHistograms[fieldIndex] = new IntHistogram(NUM_HIST_BINS, this.minPerField[fieldIndex], this.maxPerField[fieldIndex]);
	    			}
	    			this.intHistograms[fieldIndex].addValue(((IntField)field).getValue());
    			} else if (this.td.getType(fieldIndex) == Type.STRING_TYPE) {
    				if(this.stringHistograms[fieldIndex] == null) {
    					this.stringHistograms[fieldIndex] = new StringHistogram(NUM_HIST_BINS);
    				}
    				this.stringHistograms[fieldIndex].addValue(((StringField)field).getValue());
    			}
    		}
    	}
    	
    	it.close();
    }
    
    private void countTuples() throws DbException, TransactionAbortedException {
    	DbFileIterator it = Database.getCatalog().getDbFile(this.tableid).iterator(new TransactionId());
    	it.open();
    	
    	while(it.hasNext()) {
    		Tuple t = it.next();
    		this.numTuples++;
    	}
    	
    	it.close();
    }
}

    


