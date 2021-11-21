package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	private int gbField = 0;
	private Type gbFieldType = null;
	private int aggField = 0;
	private Op aggOp = null;
	private HashMap<Field, Integer> groupCounts = null;
	private HashMap<Field, Integer> groupAggs = null;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aggField = afield;
    	this.aggOp = what;
    	this.groupCounts = new HashMap<>();
    	this.groupAggs = new HashMap<>();
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
    
    private int getUpdatedAgg(Tuple tup) {
    	IntField tupleAgg = (IntField)tup.getField(this.aggField);
    	Field groupByField = this.getGroupByField(tup);
    	Integer defaultValue = (this.aggOp == Op.MIN || this.aggOp == Op.MAX) ? tupleAgg.getValue() : 0;
    	Integer currentAggValue = groupAggs.getOrDefault(groupByField, defaultValue);
    	Integer updatedAggValue = currentAggValue;
    	
    	switch (this.aggOp) {
	    	case AVG:
	    	case SUM:
	    		updatedAggValue = currentAggValue + tupleAgg.getValue();
	    		break;
	    	case MAX:
	    		updatedAggValue = Math.max(currentAggValue, tupleAgg.getValue());
	    		break;
	    	case MIN:
	    		updatedAggValue = Math.min(currentAggValue, tupleAgg.getValue());
	    		break;
	    	case COUNT:
	    		break;
	    	
    	}
    	return updatedAggValue;
    }
    
    private int getFinalAggValue(Integer count, Integer aggValue) {
    	Integer value;
    	switch (this.aggOp) {
	    	case AVG:
	    		value = aggValue / count;
	    		break;
	    	case COUNT:
	    		value = count;
	    		break;
	    	default:
	    		value = aggValue;
	    		break;
    	}
    	return value;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	Field groupByField = getGroupByField(tup);
    	
    	int updatedCount = this.getUpdatedCount(tup);
    	this.groupCounts.put(groupByField, updatedCount);
    	
    	int updatedAgg = this.getUpdatedAgg(tup);
    	this.groupAggs.put(groupByField, updatedAgg);
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
        //throw new UnsupportedOperationException("implement me");
    	TupleDesc groupAggTd;
    	ArrayList<Tuple> tuples = new ArrayList<>();
    	if (this.hasGrouping())
    		groupAggTd = new TupleDesc(new Type[] {this.gbFieldType, Type.INT_TYPE});
    	else
    		groupAggTd = new TupleDesc(new Type[] {Type.INT_TYPE});
    	
    	for (HashMap.Entry<Field, Integer> groupAggEntry: this.groupAggs.entrySet()) {
    		Tuple groupAggTuple = new Tuple(groupAggTd);
    		Integer finalAggValue = this.getFinalAggValue(this.groupCounts.get(groupAggEntry.getKey()), groupAggEntry.getValue());
    		
    		if (this.hasGrouping()) {
    			groupAggTuple.setField(0,  groupAggEntry.getKey());
    			groupAggTuple.setField(1, new IntField(finalAggValue));
    		} else
    			groupAggTuple.setField(0, new IntField(finalAggValue));
    		
    		tuples.add(groupAggTuple);
    	}
    	return new TupleIterator(groupAggTd, tuples);
    }

}
