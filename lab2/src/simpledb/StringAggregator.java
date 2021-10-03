package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	
	private int gbField = 0;
	private Type gbFieldType = null;
	private int aggField = 0;
	private Op aggOp = null;
	private HashMap<Field, Integer> groupCounts = null;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if (what != Op.COUNT)
    		throw new IllegalArgumentException();
    	
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aggField = afield;
    	this.aggOp = what;
    	this.groupCounts = new HashMap<>();
    }
    
    private boolean hasGrouping() {
    	return this.gbField != Aggregator.NO_GROUPING;
    }
    
    private Field getGroupByField(Tuple tup) {
    	if (this.hasGrouping())
    		return tup.getField(this.gbField);
    	return null;
    }
    
    private int getUpdatedCount(Tuple tup) {
    	Field groupByField = this.getGroupByField(tup);
    	int currentCount = this.groupCounts.getOrDefault(groupByField, 0);
    	return ++currentCount;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	Field groupByField = this.getGroupByField(tup);
    	int updatedCount = this.getUpdatedCount(tup);
    	groupCounts.put(groupByField, updatedCount);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	TupleDesc groupAggTd;
    	ArrayList<Tuple> tuples = new ArrayList<>();
    	if (this.hasGrouping())
    		groupAggTd = new TupleDesc(new Type[] {this.gbFieldType, Type.INT_TYPE});
    	else
    		groupAggTd = new TupleDesc(new Type[] {Type.INT_TYPE});
    	
    	for (HashMap.Entry<Field, Integer> groupAggEntry: this.groupCounts.entrySet()) {
    		Tuple groupCountsTuple = new Tuple(groupAggTd);
    		
    		if(this.hasGrouping()) {
    			groupCountsTuple.setField(0, groupAggEntry.getKey());
    			groupCountsTuple.setField(1, new IntField(groupAggEntry.getValue()));
    		}
    		else
    			groupCountsTuple.setField(0, new IntField(groupAggEntry.getValue()));
    		
    		tuples.add(groupCountsTuple);
    	}
    	
    	return new TupleIterator(groupAggTd, tuples);
    }

}
