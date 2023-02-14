package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    int fieldNum1;
    int fieldNum2;
    Predicate.Op op;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.fieldNum1 = field1;
        this.op = op;
        this.fieldNum2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        Field field1 = t1.getField(fieldNum1);
        Field field2 = t2.getField(fieldNum2);
        if(field1.compare(op, field2)){
            return true;
        }
        return false;
    }
    
    public int getField1()
    {
        return this.fieldNum1;
    }
    
    public int getField2()
    {
        return this.fieldNum2;
    }
    
    public Predicate.Op getOperator()
    {
        return this.op;
    }
}
