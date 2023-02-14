package simpledb;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class LockManager {

    //map a page to a list of tid's that have locks (shared/exclusive and granted/not) on that page
    Map<PageId, ArrayList<LockTableEntry>> lockTable;
    //Map<TransactionId, List<PageId>> transactions;
    //Map<TransactionId, List<TransactionId>> toStore;

    //inner class to set lock fields
    private class LockTableEntry {
        //don't need two boolean's for isShared and isExclusive
        boolean isExclusive;
        TransactionId tid;
        boolean isGranted;

        LockTableEntry(boolean isExclusive, TransactionId tid, boolean isGranted) {
            this.isExclusive = isExclusive;
            this.isGranted = isGranted;
            this.tid = tid;
        }
    }

    public LockManager() {
        lockTable = new HashMap<PageId, ArrayList<LockTableEntry>>();
        //transactions = new HashMap<TransactionId, List<PageId>>();
        //toStore = new HashMap<TransactionId, List<TransactionId>>();
    }

    //acquire locks
    public synchronized void acquireLock(PageId pid, TransactionId tid, boolean isExcl) throws TransactionAbortedException, InterruptedException{

        if(isLockAvailable(pid, tid, isExcl)){
            ArrayList<LockTableEntry> toAdd = lockTable.get(pid);
            LockTableEntry entry = new LockTableEntry(isExcl, tid, true);
            if(!toAdd.contains(entry)){
                toAdd.add(entry);
            }
        } else {
            ArrayList<LockTableEntry> toAdd = lockTable.get(pid);
            LockTableEntry entry = new LockTableEntry(isExcl, tid, false);
            if(!toAdd.contains(entry)){
                toAdd.add(entry);
            }
        }
        timeoutDeadlockDetector(pid, tid, isExcl);
        //System.out.println("You returned from deadlock detector.");

        //if no deadlock detected grant the lock
        lockTable.get(pid).add(new LockTableEntry(isExcl, tid, true));
        //if a deadlock was detected just quit the method, essentially aborting the current transaction
    }

    public void timeoutDeadlockDetector(PageId pid, TransactionId tid, boolean isExcl) throws InterruptedException, TransactionAbortedException {
        //give a transaction 1 second to finish (1000ms)
        long timeout = 1000;
        long waitTime = 0;
        long startTime = System.currentTimeMillis();
        while(!isLockAvailable(pid, tid, isExcl)){
            try{
                Thread.sleep(1);
                long currentTime = System.currentTimeMillis();
                long timeSoFar = currentTime - startTime;
                waitTime += timeSoFar;
                if(waitTime >= timeout){
                    throw new TransactionAbortedException();
                }
                startTime = currentTime;
            }  catch (InterruptedException ie){
                throw new InterruptedException("Interrupted exception.");
            }
        }

    }

    //a page has a lock, we need to check if that lock is free now (retry method instead of queued locks)
    public synchronized boolean isLockAvailable(PageId pid, TransactionId tid, boolean isExclusive){
        List<LockTableEntry> locks = lockTable.get(pid);
        if(locks == null){
            lockTable.put(pid, new ArrayList<LockTableEntry>());
            return true;
        }

        LockTableEntry readWriteTransition = null;
        int anotherSharedLock = -1;
        for(int i = 0; i < locks.size(); i++){
            LockTableEntry entry = locks.get(i);
            //if tid's are equal
            if(tid != null && entry.tid != null){
                if(entry.tid.equals(tid)){
                    if(entry.isGranted){
                        //matching tid's where start as R and want to change to W - update arrayList
                        if(!entry.isExclusive && isExclusive){
                            readWriteTransition = entry;
                        } else{
                            //if current lock is exclusive and we want an exclusive lock or
                            // if current lock is shared and we want shared or
                            //  if current lock is exclusive and we want shared
                            return true;
                        }
                    }
                    //if tid's do not match
                }else{
                    if(entry.isGranted){
                        //we cannot grant a lock regardless if R or W if the existing lock is W
                        if(entry.isExclusive){
                            return false;
                            //if the existing lock is shared
                        }else {
                            //The next lock might also be shared. Need to know the final
                            //shared/granted lock so that we can place this new lock in the correct sequence
                            anotherSharedLock = i;
                        }
                    }
                }
            }
        }
        //FOR READ_WRITE - adjusting list after changing previously READ_ONLY to READ_WRITE
        if(readWriteTransition != null){
            if(anotherSharedLock == -1){
                return true;
            } else {
                locks.remove(readWriteTransition);
                if(locks.size() != anotherSharedLock){
                    locks.add(anotherSharedLock + 1, new LockTableEntry(isExclusive, tid, false));
                } else {
                    locks.add(new LockTableEntry(isExclusive, tid, false));
                }
                return false;
            }
        }
        //in the case of a R transitioning to W we need to see if locks is empty and then if we want a W lock we have to deny
        if(!locks.isEmpty() && isExclusive){
            return false;
        }
        return true;
    }

    //release one lock
    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        //go to this page and remove the lock associated with this tid
        ArrayList<LockTableEntry> locks = lockTable.get(pid);
        locks.removeIf(l -> l.tid != null && l.tid.equals(tid));

    }

    //release all locks
    public synchronized void releaseAllLocks(TransactionId tid) {
        //for each pid in lockTable, get the list of locks associated with it. Need to iterate over every
        // page looking at each page for this tid, removing all locks with this tid (on every page)
        for(PageId p : lockTable.keySet()){
            ArrayList<LockTableEntry> locks = lockTable.get(p);
            //find all the tid's on this page and remove them if the tid's match
            if(tid != null)
            locks.removeIf(l -> l.tid != null && l.tid.equals(tid));
        }
    }

    //BufferPool has a holdsLock() method that checks if a specified transaction has a lock on the specified page
    public synchronized boolean holdsLock(TransactionId tid, PageId pid){
        //to check if a lock is held (i.e. granted) we need a LockTableEntry item
        //using pid, get the list of locks associated with it, for each one check for isGranted
        if (lockTable.containsKey(pid)) {
            ArrayList<LockTableEntry> lte = lockTable.get(pid);
            for (LockTableEntry l : lte) {
                //need to look at all of them before return false because could have multiple granted read locks
                if (tid!= null && l.tid != null && l.isGranted && l.tid.equals(tid)) {
                    return true;
                }
            }
        }
        return false;
    }
}
