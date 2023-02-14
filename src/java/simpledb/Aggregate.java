package simpledb;

import simpledb.TupleDesc;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    OpIterator childItr;
    int aField;
    int gField;
    Aggregator.Op aop;
    Aggregator iAgg;
    OpIterator aggItr;
    //StringAggregator sAgg;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.childItr = child;
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;
        //TupleDesc td = childItr.getTupleDesc();
        Type fieldType; //= td.getFieldType(aField);
        if(gField != -1){
            fieldType = this.childItr.getTupleDesc().getFieldType(gfield);
        } else {
            fieldType = null;
        }
        Type aggType = this.childItr.getTupleDesc().getFieldType(afield);
        if(aggType == Type.INT_TYPE){
            iAgg = new IntegerAggregator(gField, fieldType, aField, aop);
            //sAgg = null;
        } else {
            //sAgg = new StringAggregator(gField, fieldType, aField, aop);
            iAgg = new StringAggregator(gField, fieldType, aField, aop);
        }
        aggItr = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        if(this.gField == -1){
            return Aggregator.NO_GROUPING;
        }
	    return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(this.gField == -1){
            return null;
        }
        TupleDesc td = childItr.getTupleDesc();
        return td.getFieldName(this.gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        TupleDesc td = childItr.getTupleDesc();
        return td.getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {

        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {

        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        this.childItr.open();
        while(this.childItr.hasNext()){
            //grouping and aggregating
            iAgg.mergeTupleIntoGroup(this.childItr.next());
        }
        //processing the groupBy and aggregating step
        aggItr = iAgg.iterator();
        aggItr.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        //System.out.println(aggItr.hasNext());
        if(aggItr.hasNext()){
            Tuple test = aggItr.next();
            return test;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggItr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        //If GroupBy is not defined, return the tupleDesc for the iAgg
        TupleDesc td = childItr.getTupleDesc();
        String fieldName = td.getFieldName(this.aField);
        Type fieldType = td.getFieldType(aField);
        Type[] tArr;
        String[] fArr;
        if(gField == -1){
            tArr = new Type[]{fieldType};
            fArr= new String[] {aop.toString() + ": " + fieldName};
            return new TupleDesc(tArr, fArr);
        }
        //else if GroupBy is defined, return the tupleDesc for the sAgg
        Type gbFieldType = td.getFieldType(this.gField);
        String gbFieldName = td.getFieldName(this.gField);
        tArr = new Type[]{gbFieldType, fieldType};
        fArr= new String[] {gbFieldName, aop.toString() + ": " + fieldName};
        return new TupleDesc(tArr, fArr);
    }

    public void close() {
        super.close();
        this.aggItr.close();
        this.iAgg = null;
        this.childItr.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    return new OpIterator[] {this.childItr};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.childItr = children[0];
    }
    
}
