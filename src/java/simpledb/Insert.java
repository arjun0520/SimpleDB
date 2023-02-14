package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    OpIterator childItr;
    int tableId;
    TupleDesc td;
    boolean inserted = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.childItr = child;
        this.tableId = tableId;
        Type[] type = new Type[]{Type.INT_TYPE};
        String[] field = new String[]{"Number_Inserted"};
        td = new TupleDesc(type, field);
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.childItr.open();
        this.inserted = false;
    }

    public void close() {
        super.close();
        this.childItr.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.childItr.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple output;
        int counter = 0;
        if(inserted){
            return null;
        }
        while(this.childItr.hasNext()){
            try{
                Database.getBufferPool().insertTuple(tid, tableId, this.childItr.next());
            } catch (IOException io){
                throw new DbException("Insertion Failed.");
            }
            //if try didnt go to catch, insertion was successful, increase counter
            counter++;
        }
        inserted = true;
        output = new Tuple(td);
        output.setField(0, new IntField(counter));
        return output;
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
