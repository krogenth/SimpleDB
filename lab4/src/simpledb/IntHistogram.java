package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int[] buckets = null;
	private int min = 0;
	private int max = 0;
	private int numValues = 0;
	private int bucketSize = 0;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = new int[buckets];
    	this.min = min;
    	this.max = max;
    	this.bucketSize = (int)Math.ceil((max - min + 1) / (buckets * 1.0));
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	this.numValues++;
    	this.buckets[this.indexOfBucket(v)]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int index = this.indexOfBucket(v);
        double selectivity = 0.0F;
        
        switch(op) {
        case EQUALS:
        	return (this.validateBucket(index) / (this.bucketSize * 1.0)) / (this.numValues * 1.0);
        case NOT_EQUALS:
        	return (this.numValues - this.validateBucket(index) / (this.bucketSize * 1.0)) / (this.numValues * 1.0);
        case GREATER_THAN:
        	for(int i = index; i < this.buckets.length; i++) {
        		double bucketTotalPortion = this.validateBucket(i) / (this.numValues * 1.0);
        		double bucketPartition = 1;
        		if(index == i)
        			bucketPartition = (this.bucketMaxByIndex(i) - v) / (this.bucketWidthByIndex(i) * 1.0);
        		selectivity += (bucketTotalPortion * bucketPartition);
        	}
        	return selectivity;
        case GREATER_THAN_OR_EQ:
        	for(int i = index; i < this.buckets.length; i++) {
        		double bucketTotalPortion = this.validateBucket(i) / (this.numValues * 1.0);
        		double bucketPartition = 1;
        		if(index == i)
        			bucketPartition = (this.bucketMaxByIndex(i) - v + 1) / (this.bucketWidthByIndex(i) * 1.0);
        		selectivity += (bucketTotalPortion * bucketPartition);
        	}
        	return selectivity;
        case LESS_THAN:
        	for(int i = index; i > -1; i--) {
        		double bucketTotalPortion = this.validateBucket(i) / (this.numValues * 1.0);
        		double bucketPartition = 1;
        		if(index == i)
        			bucketPartition = (v - this.bucketMinByIndex(i)) / (this.bucketWidthByIndex(i) * 1.0);
        		selectivity += (bucketTotalPortion * bucketPartition);	
        	}
        	return selectivity;
        case LESS_THAN_OR_EQ:
        	for(int i = index; i > -1; i--) {
        		double bucketTotalPortion = this.validateBucket(i) / (this.numValues * 1.0);
        		double bucketPartition = 1;
        		if(index == i)
        			bucketPartition = (v - this.bucketMinByIndex(i) + 1) / (this.bucketWidthByIndex(i) * 1.0);
        		selectivity += (bucketTotalPortion * bucketPartition);
        	}
        	return selectivity;
        default:
        	return 0.0F;
        }
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
    
    private int indexOfBucket(int value) {
    	if (value < this.min)
    		return -1;
    	else if(value > this.max)
    		return this.buckets.length;
    	else
    		return (int) Math.floor((value - this.min) / (this.bucketSize * 1.0));
    	
    }
    
    private int bucketMinByIndex(int index) {
    	return this.min + (index * this.bucketSize);
    }
    
    private int bucketMaxByIndex(int index) {
    	return this.min + ((index + 1) * this.bucketSize) - 1;
    }
    
    private int bucketWidthByIndex(int index) {
    	return this.bucketMaxByIndex(index) - this.bucketMinByIndex(index) + 1;
    }
    
    private int validateBucket(int index) {
    	return (index > -1 && index < this.buckets.length) ? this.buckets[index] : 0;
    }
}
