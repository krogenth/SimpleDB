package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
    private DbIterator child;
    private int aggFieldIndex;
    private int groupByFieldIndex;
    private Aggregator.Op aggOp;
    private Aggregator agg;
    private DbIterator aggIt;
    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggFieldIndex = afield;
        this.groupByFieldIndex = gfield;
        this.aggOp = aop;
        this.agg = null;
        this.aggIt = null;
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT: 
            return "count";
        }
        return "";
    }
    
    private Aggregator createAgg(TupleDesc td) throws DbException {
        Type groupByFieldType = null;
        if (this.groupByFieldIndex != Aggregator.NO_GROUPING) {
            groupByFieldType = td.getType(this.groupByFieldIndex);
        }

        if (td.getType(this.aggFieldIndex) == Type.INT_TYPE) {
            return new IntAggregator(this.groupByFieldIndex, groupByFieldType, this.aggFieldIndex, this.aggOp);
        } else if (td.getType(this.aggFieldIndex) == Type.STRING_TYPE) {
            return new StringAggregator(this.groupByFieldIndex, groupByFieldType, this.aggFieldIndex, this.aggOp);
        } else {
            throw new DbException("This type of iterator is not supported");
        }
    }

    /**
     * Merge all tuples into our aggregator and return an iterator over the group aggregate results
     */
    private void populateAgg() throws DbException, TransactionAbortedException {
        this.child.open();

        while (this.child.hasNext()) {
            Tuple nextTuple = this.child.next();
            this.agg.merge(nextTuple);
        }
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        if (this.aggIt == null) {
            this.agg = this.createAgg(this.child.getTupleDesc());
            this.populateAgg();
            this.aggIt = this.agg.iterator();
        }
        this.aggIt.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggIt.hasNext()) {
            return this.aggIt.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggIt = this.agg.iterator();
        this.aggIt.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        String aggFieldName = this.child.getTupleDesc().getFieldName(this.aggFieldIndex);

        if (this.groupByFieldIndex == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[]{this.child.getTupleDesc().getType(this.aggFieldIndex)},
                    new String[]{aggFieldName}
            );
        } else {
            return new TupleDesc(
            	new Type[]{
            			this.child.getTupleDesc().getType(this.groupByFieldIndex),
            			this.child.getTupleDesc().getType(this.aggFieldIndex)
            	},
            	new String[]{
            			this.child.getTupleDesc().getFieldName(this.groupByFieldIndex),
            			aggFieldName
            	}
            );
        }
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.aggIt.close();
        this.agg = null;
        this.aggIt = null;
    }
}
