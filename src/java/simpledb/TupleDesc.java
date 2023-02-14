package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    //each element in 'fields' will be one object with fieldName and fieldType
    ArrayList<TDItem> fields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return fields.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        //the typeAr will be a list of types and the fieldAr will be a list of values
        //we will take typeAr[i] and fieldAr[i] to make a TupleDesc (TDItem)
        //basically, we take two arrays, combine the values to make one element (TDItem) in a
        //single array, the array indices will represent the columns for each tuple.
        //two constructors so instantiate ArrayList in each rather than globally
         fields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++){
            TDItem tpl = new TDItem(typeAr[i], fieldAr[i]);
            fields.add(tpl);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        //some code goes here
        //same as other constructor but the values will all be null
        fields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++){
            TDItem tpl = new TDItem(typeAr[i], null);
            fields.add(tpl);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //some code goes here
        return fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= fields.size()){
            throw new NoSuchElementException();
        }
        //fields is an arraylist of TdItems. Go to index i, extract TDItem.fieldName
        return fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= fields.size()){
            throw new NoSuchElementException("The given index does not exist.");
        }
        //fields is an arraylist of TdItems. Go to index i, extract TDItem.fieldType
        return fields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        //or can do for loop, get(i).fieldName() and check that way... need to do this way because
        //we need to return the index and the iterator wouldn't do that easily
        int index = -1;
        for(int i = 0; i < fields.size(); i++){
            //once we find the fieldName we want to return and search no more
            //USE .EQUALS CUZ COMPARING OBJECT!
            if( fields.get(i).fieldName != null && fields.get(i).fieldName.equals(name)){
                index = i;
                return index;
            }
        }
        //if no changes were made to index then it means that no fieldName matched
        if(index == -1){
            throw new NoSuchElementException("The field you wish to find does not exist");
        }
        //throw new NoSuchElementException("The field you wish to find does not exist");
       return -1;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //ASK:
        //There might be multiple tuples (not necessarily in order) that have the same TupleDesc. We want
        //to return the sum (size of bytes) for all tuples that match that description? If tuples from
        //a given tupleDesc are of a fixed size (i.e 2 bytes) and we have 6 tuples that match that tuple
        //description we would have a size of 12 bytes?? YES
        //we are adding up the byte size of every tupleDesc in a given tuple (NOT looking for matching
        //tupleDesc because we aren't given a tupleDesc to compare to)
        //loop through 'fields' and find all tupleDesc
        int bytes = 0;
        for(int i = 0; i < this.fields.size(); i++){
            bytes += fields.get(i).fieldType.getLen();
        }
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //create a new tuple desc. So if you have two tuples to merge:
        // <int(age), string(name)> + <string(color), string(food)>
        //you would end up with one tuple w/ four attributes
        // <int(age), string(name), string(color), string(food)>
        Type[] typeArray = new Type[td1.numFields() + td2.numFields()];
        //use getSize() for fields because we need to make sure we allocate enough bytes not quantity
        String[] fieldArray = new String[td1.getSize() + td2.getSize()];
        for(int i = 0; i < td1.numFields(); i++){
            typeArray[i] = td1.fields.get(i).fieldType;
            fieldArray[i] = td1.fields.get(i).fieldName;
        }
        //can't start the for loop with i = # > than td2 if td2 has fewer elements than td1.... Must start w/ i = 0
        for(int i = 0; i < td2.numFields(); i++){
            typeArray[i + td1.numFields()] = td2.fields.get(i).fieldType;
            fieldArray[i + td1.numFields()] = td2.fields.get(i).fieldName;
        }
        return new TupleDesc(typeArray, fieldArray);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        //Need to make sure that o is a tupleDesc
        //if it is then we need to make sure number of elements are equal
        //and iterate through each element making sure o type = tplDesc type
        if(o instanceof TupleDesc){
            TupleDesc tplD = (TupleDesc) o;
            //if sizes are equal
            if(this.fields.size() == tplD.fields.size()){
                //if any types dont match return false
                for(int i = 0; i < this.fields.size(); i++){
                    if(tplD.getFieldType(i) != this.getFieldType(i)){
                        return false;
                    }
                }
                return true;
            }
        }
        //if o is not a tupleDesc
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String output = "< ";
        for(int i = 0; i < fields.size(); i++){
            output += fields.get(i).fieldType + " (" + fields.get(i).fieldName + ")\n ";
        }
        output += ">";
        return output;
    }
}
