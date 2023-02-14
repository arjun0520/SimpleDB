package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    RecordId rid;
    ArrayList<Field> tpl;
    TupleDesc tplDesc;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        //we pass in a tupleDesc which is an array of Types OR an array of values and types
            //need to handle both cases NO
        //just creating an empty Tuple
        this.tplDesc = td;
        tpl = new ArrayList<Field>();
        rid = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tplDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        //ASK TA:
        //I'm using .add() to make the tests pass for checkpoint 1. If I understand what the TA said on my Ed post,
        //using add shifts existing elements to the right (unless i happens to be the last index). This means that
        //we are adding a field but now the tupleDesc will be wrong for this tuple because we didn't also add a new
        //tupleDesc. I don't think that .add() satisfies what the method documentation states either. It states that
        //we need to change the value of the ith field. That suggests that we are taking an existing field and swapping
        //out the original value for this new value, f. In that case, we should never be adding a new element. However,
        //now this doesn't align with what the second TA said on my EdPost... that we can't use .set() because the
        //element doesn't exist. GET CLARIFICATION ON WHETHER OR NOT THE METHOD DOCUMENTATION IS CORRECT!
            //might need to use resetTupleDesc()
        tpl.add(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        //go to tpl[i] and extract the value part of the object stored there
        if(i < 0 || i > tpl.size()){
            throw new IndexOutOfBoundsException();
        } else {
            return tpl.get(i);
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
       String output = "";
       for(int i = 0; i < tpl.size() - 1; i++){
           output += tpl.get(i).toString() + "\t";
       }
       output += tpl.get(tpl.size() - 1).toString();
       return output;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return tpl.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tplDesc = td;
    }
}
