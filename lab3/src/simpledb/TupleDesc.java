package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	private ArrayList<Type> types = null;
	private ArrayList<String> names = null;
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
    	ArrayList<Type> typeArray = new ArrayList<>();
    	ArrayList<String> nameArray = new ArrayList<>();
    	
    	typeArray.addAll(td1.types);
    	typeArray.addAll(td2.types);
    	
    	nameArray.addAll(td1.names);
    	nameArray.addAll(td2.names);
    	
        return new TupleDesc(typeArray.toArray(new Type[typeArray.size()]), nameArray.toArray(new String[nameArray.size()]));
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
    	this.types = new ArrayList<Type>(Arrays.asList(typeAr));
    	this.names = new ArrayList<String>(Arrays.asList(fieldAr));
    	
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
    	this.types = new ArrayList<Type>(Arrays.asList(typeAr));
    	this.names = new ArrayList<String>(Arrays.asList(new String[this.numFields()]));
    	
    	for (Type var : types) {
    		this.size += var.getLen();
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.types.size();
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
    		return this.names.get(i);
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
    	for(int i = 0; i < this.names.size(); i++) {
    		if (this.names.get(i) != null) {
	    		if (this.names.get(i).equals(name))
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
    		return this.types.get(i);
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
    	
    	for (int i = 0; i < this.numFields(); i++) {
            if (this.getType(i) != ((TupleDesc)o).getType(i)) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	return Objects.hash(Arrays.hashCode(this.types.toArray(new Type[this.types.size()])), Arrays.hashCode(this.names.toArray(new String[this.names.size()])), this.size);
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
    		joiner.add(this.types.get(i) + "(" + this.names.get(i) + ")");
    	}
        return joiner.toString();
    }
}
