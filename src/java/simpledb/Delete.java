package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    OpIterator childItr;
    TupleDesc td;
    boolean inserted = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.childItr = child;
        Type[] type = new Type[]{Type.INT_TYPE};
        String[] field = new String[]{"Number_Deleted"};
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
        //inserted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple output;
        int counter = 0;
        if(inserted){
            return null;
        }
        while(this.childItr.hasNext()){
            try{
                Database.getBufferPool().deleteTuple(tid, this.childItr.next());
            } catch (IOException io){
                throw new DbException("Deletion Failed.");
            }
            //if try didnt go to catch, deletion was successful, increase counter
            counter++;
        }
        inserted = true;
        output = new Tuple(td);
        output.setField(0, new IntField(counter));
        return output;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.childItr};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.childItr = children[0];
    }

}
