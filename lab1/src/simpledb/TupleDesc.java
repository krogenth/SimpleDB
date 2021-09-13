package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	private Type[] types;
	private String[] names;
	private int size = 0;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	Type[] typeArray = Arrays.copyOf(td1.types,  td1.numFields() + td2.numFields());
    	System.arraycopy(td2.types, 0, typeArray, td1.numFields(), td2.numFields());
    	
    	String[] nameArray = Arrays.copyOf(td1.names,  td1.numFields() + td2.numFields());
    	System.arraycopy(td2.names,  0,  nameArray, td1.numFields(), td2.numFields());
        return new TupleDesc(typeArray, nameArray);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	this.types = Arrays.copyOf(typeAr,  typeAr.length);
    	this.names = Arrays.copyOf(fieldAr,  fieldAr.length);
    	
    	for (Type var : types) {
    		this.size += var.getLen();
    	}
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.types = Arrays.copyOf(typeAr,  typeAr.length);
    	this.names = new String[typeAr.length];
    	
    	for (Type var : types) {
    		this.size += var.getLen();
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < numFields())
    		return this.names[i];
    	else
    		throw new NoSuchElementException();
    		
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
    	for(int i = 0; i < this.names.length; i++) {
    		if (this.names[i] != null) {
	    		if (this.names[i].equals(name) == true)
	    			return i;
    		}
    	}
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < numFields())
    		return this.types[i];
    	else
    		throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return this.size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if (o == null)
    		return false;
    	
    	if (!(o instanceof TupleDesc))
    		return false;
    	
    	if (o == this)
    		return true;
    	
    	if (((TupleDesc)o).numFields() != this.numFields())
    		return false;
    	
        return this.hashCode() == ((TupleDesc)o).hashCode();
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	return Objects.hash(Arrays.hashCode(this.types), Arrays.hashCode(this.names), this.size);
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringJoiner joiner = new StringJoiner(", ");
    	for (int i = 0; i < this.numFields(); i++) {
    		joiner.add(this.types[i] + "(" + this.names[i] + ")");
    	}
        return joiner.toString();
    }
}
