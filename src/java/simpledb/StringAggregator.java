package simpledb;

import simpledb.TupleDesc;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbField;
    Type gbFieldType;
    int aField;
    Op what;
    Map<Field, Integer> groupByDefined;
    //int[] valuesOnly = new int[1];
    Tuple forIterator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.aField = afield;
        this.gbFieldType = gbfieldtype;
        this.what = what;
        this.groupByDefined = new HashMap<Field, Integer>();
        //this.valuesOnly[0] = 0;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        forIterator = tup;
        //Get the value of the tuple from Tuple (i.e JOHN)
        Field tupsField = tup.getField(gbField);
        if(gbField != -1){
            tupsField = tup.getField(gbField);
            if(groupByDefined.containsKey(tupsField)){
                //If JOHN is already in map and has been seen 3 times, we want to change it
                //to 4 times for this tuple
                groupByDefined.put(tupsField, groupByDefined.get(tupsField) + 1);
                //want to increment the current value by 1
            }else {
                //otherwise if JOHN is not in the map we want to put counter in
                groupByDefined.put(tupsField, 1);
            }
        } else {
            tupsField = null;
            if(groupByDefined.containsKey(tupsField)){
                //If JOHN is already in map and has been seen 3 times, we want to change it
                //to 4 times for this tuple
                groupByDefined.put(tupsField, groupByDefined.get(tupsField) + 1);
                //want to increment the current value by 1
            }else {
                //otherwise if JOHN is not in the map we want to put counter in
                groupByDefined.put(tupsField, 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<simpledb.Tuple> tuples = new ArrayList<simpledb.Tuple>();
        Type[] tArr;
        String[] sArr;
        Tuple tpl;
        TupleDesc td;
        if(gbField != -1){
            //type can be any type
            tArr = new Type[] {gbFieldType, Type.INT_TYPE};
            sArr = new String[] {"groupVal", "aggregateValue"};
            td = new TupleDesc(tArr, sArr);
        } else {
            sArr = new String[] {"aggregateVal"};
            td = new TupleDesc(new Type[] {Type.INT_TYPE}, sArr);
        }
        //should be able to do with only one map (no array) because we only have one possible agg function
        for(Field f : groupByDefined.keySet()){
            tpl = new simpledb.Tuple(td);
            int intAggField = groupByDefined.get(f);
            if(gbField != -1){
                tpl.setField(0, f);
                //use IntField because we are counting!
                tpl.setField(1, new IntField(groupByDefined.get(f)));
            }else {
                tpl.setField(0, new IntField(intAggField));
            }
            tuples.add(tpl);
        }
        return new TupleIterator(td, tuples);
    }
}
