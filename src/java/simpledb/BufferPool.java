package simpledb;

import simpledb.Database;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;

import java.lang.reflect.Array;
import java.util.ArrayList;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    ConcurrentHashMap<PageId, Page> bp;
    int maxPages;
    LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.bp = new ConcurrentHashMap<PageId, Page>();
        this.maxPages = numPages;
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        try {
            if (perm.equals(Permissions.READ_ONLY)) {
                lockManager.acquireLock(pid, tid, false);
            } else {
                lockManager.acquireLock(pid, tid, true);
            }
        } catch (InterruptedException ie){
        }


        PageId copyOfPid = pid;
        if(bp.containsKey(copyOfPid)){
            return bp.get(copyOfPid);
        }else {
            if(bp.size() >= maxPages){
                evictPage();
            }
            Page pg = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            bp.put(pid, pg);
            return pg;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
       //write to disk only after commit (NO STEAL)
        //get all pageId's associated with this tid, need to save changes and flush to disk
        for (PageId pid : bp.keySet()) {
            Page p = bp.get(pid);
            //for each pid, if commit is true and this tid is the tid that modified the page
            TransactionId dirtyTid = p.isDirty();
            if (dirtyTid != null && tid.equals(p.isDirty())) {
                //this tid modified this pid and commit = true -> flush to disk
                if (commit) {
                    Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
                    Database.getLogFile().logCommit(tid);
                    if(holdsLock(tid, pid)){
                        p.setBeforeImage();
                    }
                } else {
                //otherwise, the page wasn't modified so we just remove this pg from bp because it matches disk
                    discardPage(pid);
                }
            }
        }
        //after flushing/discarding all pages associated with this tid, release all locks.
        lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        //Make a copy of the file where the table can be found using tableId
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        //call HeapFile to insert each tuple on that HeapFile and put inserted tuples into this arrayList
        ArrayList<Page> pages = file.insertTuple(tid, t);
        //for each page that was modified
        for(Page p : pages){
            //mark it as dirty (because its been changed)
            p.markDirty(true, tid);
            //Call evictPage THEN put the modified page into the BP. This clears the original page (if
            //it was even there to begin with) from BP and replaces with new.
            //add as part of Lab2:
            //evictPage();
            bp.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        //literally just use from insertTuple (except don't have tableID)
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = file.deleteTuple(tid, t);
        for(Page p : pages){
            p.markDirty(true, tid);
            bp.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        //call flushPage() to execute
        for(PageId pid : bp.keySet()){
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bp.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        //check to make sure this pid actually exists on this page
        if(bp.containsKey(pid)){
            Page pg = bp.get(pid);
            if(pg.isDirty() != null){
                //get the tid to check if dirty or not
                TransactionId tid = pg.isDirty();
                //append an update record to the log, with
                //a before-image and after-image.
                if (tid != null){
                    Database.getLogFile().logWrite(tid, pg.getBeforeImage(), pg);
                    Database.getLogFile().force();
                }
                //we only want to write to disk and change to NOT dirty if is already dirty
                if(tid != null){
                    //get the HeapFile
                    DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    //write to disk
                    file.writePage(pg);
                    //change Dirty = false
                    pg.markDirty(false, tid);
                }
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        //look at all the pages in the bufferPool
        for(PageId pid : bp.keySet()){
            Page pg = bp.get(pid);
            //determine if this pid is locked by this tid
            if(holdsLock(tid, pid)){
                pg.setBeforeImage();
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                //write to disk
                file.writePage(pg);
                //change Dirty = false
                pg.markDirty(false, tid);
              lockManager.releaseLock(pid, tid);
          }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageId tryThisPid = null;
        //look at every page trying to find one that hasn't been modified
        for(PageId pid : bp.keySet()){
            Page pg = bp.get(pid);
            //if the page is not dirty, we can try to evict it
            if(pg.isDirty() == null){
                tryThisPid = pid;
            }
        }
        //otherwise if all pages were dirty
        if(tryThisPid == null){
            //evict the first page
            ArrayList<PageId> pid = new ArrayList<PageId>(bp.keySet());
            tryThisPid = pid.get(0);
        }
        try{
            flushPage(tryThisPid);
            discardPage(tryThisPid);
        } catch (IOException io){
            throw new DbException("Eviction Failed.");
        }
    }
}
