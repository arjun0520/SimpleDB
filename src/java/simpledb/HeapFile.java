package simpledb;

import simpledb.Database;
import simpledb.HeapPageId;
import simpledb.Permissions;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    TupleDesc td;
    File f;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        //File is a standard Java class, not specific to this lab
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        //DbFile is for files on disk. Can fetch pages and iterate through tuples. Have unique id (pid)
            //DbFiles are generally accessed through the buffer pool, rather than directly by operators.
        try {
            RandomAccessFile randFile = new RandomAccessFile(this.f, "r");
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] pageInBytes = new byte[BufferPool.getPageSize()];
            randFile.seek(offset);
            randFile.readFully(pageInBytes);
            randFile.close();
            return new HeapPage((HeapPageId) pid, pageInBytes);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not legal argument.");
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();
        int offset = BufferPool.getPageSize() * pgNo;
        RandomAccessFile randFile = new RandomAccessFile(this.f, "rw");
        try{
            randFile.seek(offset);
            randFile.write(page.getPageData());//,0, BufferPool.getPageSize());
            randFile.close();
        }catch (IOException io) {
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        //don't insert if the TupleDesc doesn't match this HeapFile's
        if(!this.td.equals(t.getTupleDesc())){
            throw new DbException("The tupleDesc's dont match.");
        }
        ArrayList<Page> pages = new ArrayList<Page>();
        //search all the pages on this HeapFile for an empty slot
            //must search ALL pages on the HeapFile BEFORE making a new page
        for(int i = 0; i < this.numPages(); i++){
            //make a HeapPageId for each page (i)
            HeapPageId pid = new HeapPageId(getId(), i);
            //use this pid to get that page from BP
            HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            //check to see if there are empty slots on the page. If there are not, just look to the next
            //page, we don't want to create a new page until we are sure ALL of the pages are full
            if(hPage.getNumEmptySlots() != 0){
                //if we found an empty slot, we want to use that page, but we have to change the permissions to R/W
                hPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                //then we can insert our tuple onto the page
                hPage.insertTuple(t);
                //and then we can add the page to the arrayList
                pages.add(hPage);
                return pages;
            }
            //TODO  - called getPage() above, now need to release
            if (Database.getBufferPool().holdsLock(tid, pid)) {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }

        //now we've searched all pages and found zero empty slots, make a new page
        HeapPageId pid = new HeapPageId(getId(), numPages());
        //use HeapPages's createEmptyPageData() to make an empty page
        HeapPage pg2 = new HeapPage(pid, HeapPage.createEmptyPageData());
        //writePage(hPage);
       // HeapPage pg2 = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        //insert the tuple into that page
        pg2.insertTuple(t);
        writePage(pg2);
        //add the page to the arrayList
        pages.add(pg2);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<Page>();
        if(t.getRecordId().getPageId().getTableId() == getId()){
            HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
            hPage.deleteTuple(t);
            pages.add(hPage);

            //TODO  - called getPage() above, now need to release
            if (Database.getBufferPool().holdsLock(tid, t.getRecordId().getPageId())) {
                Database.getBufferPool().releasePage(tid, t.getRecordId().getPageId());
            }
        } else {
            throw new DbException("The tuple does not exists on this page/file.");
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        TransactionId tid;
        Permissions p = Permissions.READ_ONLY;
        Iterator<Tuple> tplIterator;
        HeapFile f;
        //when reading a page we start at page 0...
        int currentPage;
        boolean opened = false;

        private HeapFileIterator(HeapFile f, TransactionId tid){
            this.f = f;
            this.tid = tid;
        }

        public void open() throws simpledb.DbException, simpledb.TransactionAbortedException {
            //per the spec: Do not load the entire table when call this.
                //I think this just means that "open" means to create a new, fresh iterator to
                //iterate over a page
                    //to iterate over a page we have to get the pageId
            currentPage = 0;
            HeapPageId hpid = new HeapPageId(f.getId(), currentPage);
            HeapPage hPage = (HeapPage) Database.getBufferPool().getPage(tid, hpid , p);
            tplIterator = hPage.iterator();
            opened = true;
        }

        public boolean hasNext() throws simpledb.DbException, simpledb.TransactionAbortedException {
            //check to see if there are more tuples in a given DbFile
                //DbFileIterator has shell method for hasNext() and the comments say to return false
                //also if iterator isn't opened? Thats the test I was failing...
                //Create "opened" boolean variable, set to false as default and set to true in open()
            if (!opened) return false;
            if (tplIterator.hasNext()) return true;

            currentPage++;

            while (currentPage < numPages()) {
                HeapPage hpg = (HeapPage) Database.getBufferPool().getPage(
                        tid, new HeapPageId(getId(), currentPage), p);
                tplIterator = hpg.iterator();
                if (tplIterator.hasNext()) return true;
                else currentPage++;
            }
            return false;

        }

        public Tuple next() throws simpledb.DbException, simpledb.TransactionAbortedException, NoSuchElementException {
            //Per Oracle spec, Iterator .hasNext() returns true if .next throws an error when
            //there are no more elements
            /*
            if(hasNext() && tplIterator.hasNext()){
                        return tplIterator.next();
            }
            throw new NoSuchElementException();*/
            if(!opened && !hasNext()){
                throw new NoSuchElementException();
            }
            return tplIterator.next();
        }

        public void rewind() throws simpledb.DbException, simpledb.TransactionAbortedException {
            //set currentPage back to 0 and put iterator back at start
            this. currentPage = 0;
            this.open();
        }

        public void close() {
            //Difference between rewind() and close()??
            //in rewind() we call open to return a fresh iterator but in close() we can just set
            //to null because we don't want to use it anymore.
            tplIterator = null;
            currentPage = 0;
            opened = false;
        }
    }
}

