package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	private TransactionId tid = null;
	private int tableid = 0;
	private String tableName = null;
	private DbFile table = null;
	private DbFileIterator it = null;
	
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableName = tableAlias;
    	this.table = Database.getCatalog().getDbFile(this.tableid);
    	this.it = this.table.iterator(this.tid);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        // some code goes here
    	this.it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	if (this.it == null)
    		return null;
    	TupleDesc temp = Database.getCatalog().getTupleDesc(this.tableid);
    	Type[] typeArr = new Type[temp.numFields()];
    	String[] nameArr = new String[temp.numFields()];
    	for (int i = 0; i < temp.numFields(); i++) {
    		typeArr[i] = temp.getType(i);
    		nameArr[i] = this.tableName + "." + temp.getFieldName(i);
    	}
    	temp = new TupleDesc(typeArr, nameArr);
    	return temp;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (this.it == null)
    		throw new DbException("Iterator was null");
    	
    	return this.it.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
    	if (this.it == null)
    		throw new DbException("Iterator was null");
    	
    	return this.it.next();
    }

    public void close() {
        // some code goes here
    	this.it.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
    	this.close();
    	this.open();
    }
}
