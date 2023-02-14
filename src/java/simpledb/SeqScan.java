package simpledb;

import simpledb.Database;
import simpledb.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    TransactionId tranId;
    int tblId;
    String tblAlias;
    //change HeapFile to DbFile????
    HeapFile f;
    DbFileIterator itr;
    //use 'open' variable like in HeapFileIterator
    boolean opened = false;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tranId = tid;
        this.tblId = tableid;
        this.tblAlias = tableAlias;
        //reads each tuple of a table, so it needs to have a DbFile and a DbFile iterator
        this.f = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tblId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.tblAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        //ASK: 'reset' tells me I need to set both parameters to 0 (or null) but the fact that
        //reset() is passed parameters tells me that I should be "changing" not "resetting"
        //variables to these parameters....
        this.tblId = tableid;
        this.tblAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        //'open' means we want to start iterating through a DbFile
        //this means we need to instantiate our DbFileitr here
            //*** Per spec, need to use HeapFile NOT DbFile
        itr = f.iterator(this.tranId);
        itr.open();
        opened = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        //get the tupleDesc (field names) from
        TupleDesc td = f.getTupleDesc();
        //prefix the tupleDesc with the tableAlias, separated with "."
            //should be for each fieldName
        //Each tupleDesc has fieldName / FieldType so need to make two arrays because need
        //to separate name from type so can concatenate tableAlias to each name and then
        //rejoin the new Alias.FieldName / FieldType --> call TupleDesc constructor!
        String[] names = new String[td.numFields()];
        Type[] types = new Type[td.numFields()];
        for(int i = 0; i < names.length; i++){
            String n = td.getFieldName(i);
            Type t = td.getFieldType(i);
            names[i] = tblAlias + "." + n;
            types[i] = t;
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.itr.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(itr == null){
            throw new NoSuchElementException();
        }
        return this.itr.next();
    }

    public void close() {
        this.itr.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.itr.rewind();
    }
}
