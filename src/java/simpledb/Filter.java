package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    Predicate p;
    OpIterator childItr;
    boolean opened = false;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.childItr = child;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return childItr.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        //NEED TO USE SUPER.OPEN()
        super.open();
        childItr.open();
        opened = true;
    }

    public void close() {
        super.close();
        childItr.close();
        opened = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childItr.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        //copied over from Project class
        while (opened && childItr.hasNext()) {
            //fetch the next tuple
            Tuple t = childItr.next();
            //apply the predicate and return tuple if pass predicate
            if(this.p.filter(t)){
                return t;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // If there is only one child, return an array of only one element.
        return new OpIterator[]{this.childItr};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // If the operator has only one child, children[0] should be used
        if(this.childItr != children[0]){
            this.childItr = children[0];
        }
    }

}
