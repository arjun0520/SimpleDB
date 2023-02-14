package simpledb;

import simpledb.Tuple;
import simpledb.TupleDesc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbField;
    int aField;
    Type gbFieldType;
    Op what;
    //to map Fields to aggregate values when group by is defined
    Map<Field, Integer> groupByDefined;
    //if group by is not defined we only need an array with 1 element to hold aggregate values
    Integer[] valuesOnly;
    //when group by is defined, we need to count the number of occurrences for each Field to use when what = AVG
    Map<Field, Integer> forAverage;
    //to use for AVG when group by is not defined
    int numCounted = 0;
    //created so would have access to the field type in the iterator without having to create a new tuple
    Field tupleForIterator;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.groupByDefined = new HashMap<Field, Integer>();
        this.valuesOnly = new Integer[]{0};
        this.forAverage = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        tupleForIterator = tup.getField(aField);
        //Get the value of the tuple from Tuple (i.e JOHN)
        Field tupsField;
        //Get the value of the aggregate field (i.e AGE - 20)
        IntField aggField = (IntField) tup.getField(aField);
        //turn that age into an Integer so we can perform math on it
        int intAggField = aggField.getValue();
        //for those aggregates that involve a previous value
        Integer currentValue;
    //If gbField is DEFINED
        if(gbField != -1){
            tupsField = tup.getField(gbField);
            if(what == Aggregator.Op.SUM){
                //The map already has JOHN - some age. We need to get 'some age' and add this
                //tups age.
                if (groupByDefined.containsKey(tupsField)){
                    //for those aggregates that involve a previous value
                        //NEED TO .get(TUPSFIELD) NOT aggField -> fixes iterator test
                    currentValue = groupByDefined.get(tupsField);
                    //using .replace() so that we don't end up with multiple JOHN's
                    groupByDefined.put(tupsField, currentValue + intAggField);
                }else {
                    //This is the first JOHN in the map, use .put()
                    groupByDefined.put(tupsField, intAggField);
                }
            }
            if(what == Aggregator.Op.COUNT){
                if(groupByDefined.containsKey(tupsField)){
                    //If JOHN is already in map and has been seen 3 times, we want to change it
                    //to 4 times for this tuple
                    groupByDefined.put(tupsField, groupByDefined.get(tupsField) + 1);
                }else {
                    //otherwise if JOHN is not in the map we want to put counter in
                    groupByDefined.put(tupsField, 1);
                }

            }
            if(what == Aggregator.Op.AVG){
                //I don't think this will work, we can't take the average every time or
                //it wont be accurate. We need to sum and then once we have read all tuples we
                //can divide by number of tuples for each field.
                    //this means that Aggregate will handle the Average calculation
                    //we just need to give Aggregate access to the number of ages summed for each
                    //gbField
                if(groupByDefined.containsKey(tupsField)){
                    currentValue = groupByDefined.get(tupsField);
                    groupByDefined.put(tupsField, currentValue + intAggField);
                    forAverage.put(tupsField, forAverage.get(tupsField) + 1);
                } else {
                    groupByDefined.put(tupsField, intAggField);
                    forAverage.put(tupsField, 1);
                }

            }
            if(what == Aggregator.Op.MIN){
                if(groupByDefined.containsKey(tupsField)){
                    currentValue = groupByDefined.get(tupsField);
                    groupByDefined.put(tupsField, Math.min(currentValue, intAggField));
                } else {
                    groupByDefined.put(tupsField, intAggField);
                }

            }
            if(what == Aggregator.Op.MAX){
                if(groupByDefined.containsKey(tupsField)){
                    currentValue = groupByDefined.get(tupsField);
                    groupByDefined.put(tupsField, Math.max(currentValue, intAggField));
                }else {
                    groupByDefined.put(tupsField, intAggField);
                }
            }
        } else {
            tupsField = null;
    //if gbField is NOT DEFINED
            if(what == Aggregator.Op.SUM){
                valuesOnly[0] += intAggField;
            }
            if(what == Aggregator.Op.COUNT){
                valuesOnly[0]++;
            }
            if(what == Aggregator.Op.AVG){
                valuesOnly[0] += intAggField;
                //will need to access numCounted in Aggregate to finish calculating AVG
                numCounted++;
            }
            if(what == Aggregator.Op.MIN){
                valuesOnly[0] = Math.min(valuesOnly[0], intAggField);
            }
            if(what == Aggregator.Op.MAX){
                valuesOnly[0] = Math.max(valuesOnly[0], intAggField);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Type[] tArr;
        String[] sArr;
        TupleDesc td;
        Tuple tp;
        Type aggType = tupleForIterator.getType();
        if(gbField != -1){
            //type can be any type
            tArr = new Type[] {gbFieldType, Type.INT_TYPE};
            sArr = new String[] {"groupVal", "aggregateValue"};
            td = new TupleDesc(tArr, sArr);
        } else {
            sArr = new String[] {"aggregateVal"};
            td = new TupleDesc(new Type[] {Type.INT_TYPE}, sArr);
        }
        //We pull from groupByDefined is gbfield != -1 and we pull from the values array otherwise
        if (gbField != -1) {
            for(Field f : groupByDefined.keySet()){
                tp = new Tuple(td);
                int intAggField = groupByDefined.get(f);
                if(what == Aggregator.Op.AVG){
                    tp.setField(0, f);
                    tp.setField(1, new IntField(intAggField / forAverage.get(f)));
                } else {
                    tp.setField(0, f);
                    tp.setField(1, new IntField(intAggField));
                }
                //System.out.println("IntegerAggregator: " + tp.toString());
                tuples.add(tp);

            }
        } else {
            tp = new Tuple(td);
            //if gbField is NOT DEFINED
            IntField iF = new IntField(valuesOnly[0]);
            int avgCalculation = valuesOnly[0] / numCounted;
            if(what == Aggregator.Op.AVG){
                tp.setField(0, new IntField(avgCalculation));
            } else {
                tp.setField(0, iF);
            }
           tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }
}
