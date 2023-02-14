# CSE444 Lab 4: Rollback and Recovery

**Due: Tuesday, March 8th 2022**

## Introduction

In this lab you will implement log-based rollback for aborts and log-based crash recovery. We supply you with the code that defines the log format and appends records to a log file at appropriate times during transactions. You will implement rollback and recovery using the contents of the log file.

The logging code we provide generates records intended for physical whole-page undo and redo. When a page is first read in, our code remembers the original content of the page as a before-image. When a transaction updates a page, the corresponding log record contains that remembered before-image as well as the content of the page after modification as an after-image. You'll use the before-image to roll back during aborts and to undo loser transactions during recovery, and the after-image to redo winners during recovery.

We are able to get away with doing whole-page physical UNDO (while ARIES must do logical UNDO) because we are doing page level locking and because we have no indices which may have a different structure at UNDO time than when the log was initially written. The reason page-level locking simplifies things is that if a transaction modified a page, it must have had an exclusive lock on it, which means no other transaction was concurrently modifying it, so we can UNDO changes to it by just overwriting the whole page.

Your BufferPool already implements abort by deleting dirty pages, and pretends to implement atomic commit by forcing dirty pages to disk only at commit time. Logging allows more flexible buffer management (STEAL and NO-FORCE), and our test code calls `BufferPool.flushAllPages()` at certain points in order to exercise that flexibility.

## 1\. Getting started

You should begin with the code you submitted for Lab 3 (if you did not submit code for Lab 3, or your solution didn't work properly, contact us to discuss catching up.)

#### Initial lab code

As before, you'll need to pull from the lab 4 upstream branch:

```sh
$ git pull upstream lab4   # Pull lab4!
```

#### Logging modifications

You will next need to make the following changes to your existing code:

1. Insert the following lines into `BufferPool.flushPage()` before your call to `writePage(p)`, where `p` is a reference to the page being written:

```java
    // append an update record to the log, with
    // a before-image and after-image.
    TransactionId dirtier = p.isDirty();
    if (dirtier != null){
      Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
      Database.getLogFile().force();
    }
```

This causes the logging system to write an update to the log. We force the log to ensure the log record is on disk before the page is written to disk.

Now modify the code above to also check that the "dirtier" (i.e. the transaction that made this page dirty) is still executing. If the dirtier is executing, we want to log the change. If the dirtier
already committed, then we don't need to log the change because we already logged it before writing the COMMIT log record.

3. *STEAL:* You can now modify `BufferPool.evictPage()` to allow it to flush any page to disk.

4.  *NO FORCE:* Your `BufferPool.transactionComplete()` calls `flushPage()` for each page that a committed transaction dirtied. Remove the calls to `flushPage()`. We no longer need to force pages to disk at commit time. However, for each of these dirtied pages, we now need to add a call to `logWrite(tid, p.getBeforeImage(), p)` and then, of course, force the log to disk. We need to make sure that we can redo the changes to all dirtied pages even if they were never flushed to disk. Finally, add a call to `p.setBeforeImage()`.

        // use current page contents as the before-image
        // for the next transaction that modifies this page.
        p.setBeforeImage();

After an update is committed, a page's before-image needs to be updated so that later transactions that abort rollback to this committed version of the page. (Note: We can't just call setBeforeImage() in flushPage(), since flushPage() might be called even if a transaction isn't committing. Our test case actually does that!)

The last point to ensure is that the COMMIT log record is written after all the update log records above. This should already happen in `Transaction.transactionComplete()`. Make sure that it does.

5. After you have made these changes, do a clean build (`ant clean; ant compile` from the command line, or a "Clean" from the "Project" menu in Eclipse.)

6. At this point your code should pass the first *two* sub-tests of the `LogTest` systemtest, and fail the rest:

```sh
% ant runsystest -Dtest=LogTest
    ...
    [junit] Running simpledb.systemtest.LogTest
    [junit] Testsuite: simpledb.systemtest.LogTest
    [junit] Tests run: 10, Failures: 0, Errors: 8, Time elapsed: 0.42 sec
    [junit] Tests run: 10, Failures: 0, Errors: 8, Time elapsed: 0.42 sec
    [junit]
    [junit] Testcase: PatchTest took 0.057 sec
    [junit] Testcase: TestFlushAll took 0.022 sec
    [junit] Testcase: TestCommitCrash took 0.018 sec
    [junit]     Caused an ERROR
    [junit] LogTest: tuple not found
    [junit] Testcase: TestAbort took 0.03 sec
    [junit]     Caused an ERROR
    [junit] LogTest: tuple present but shouldn't be
    ...
```

7. If you don't see the above output from `ant runsystest -Dtest=LogTest`, something is somehow incompatible with your existing code. You should figure out and fix the problem before proceeding; ask us for help if necessary.

**Important:** Once you switch policies from FORCE to NO FORCE, it's possible a few of your lab 3 tests might not pass. This is okay because we are changing to a different set of policies in lab4. You only need to make sure the lab 4 tests pass to get full credit.

## 2\. Rollback

Read the comments in `LogFile.java` for a description of the log file format. You should see in `LogFile.java` a set of functions, such as `logCommit()`, that generate each kind of log record and append it to the log.

Your first job is to implement the `rollback()` function in `LogFile.java`. This function is called when a transaction aborts, before the transaction releases its locks. Its job is to undo any changes the transaction may have made to the database.

Your `rollback()` should read the log file, find all update records associated with the aborting transaction, extract the before-image from each, and write the before-image to the table file on disk. Use `raf.seek()` to move around in the log file, and `raf.readInt()` etc. to examine it. Use `readPageData()` to read each of the before- and after-images. You can use the map `tidToFirstLogRecord` (which maps from a transaction id to an offset in the heap file) to determine where to start reading the log file for a particular transaction. You will need to make sure that you discard any page from the buffer pool whose before-image you write back to the table file.

As you develop your code, you may find the `Logfile.print()` method useful for displaying the current contents of the log.

Design suggestion: You do not have to, but you may want to add compensating log records to the log file. These log records will
record actions that compensate previous updates by aborted transactions. You should write those log records to disk before the final
abort log record for the transaction. While you can implement the entire lab without compensating log records, these log records can make recovery significantly simpler to implement. We leave this design choice up to you.


#### Exercise 1: LogFile.rollback()

You are now ready to implement LogFile.rollback().  After completing this exercise, you should be able to pass the `TestAbort` and `TestAbortCommitInterleaved` sub-tests of the `LogTest` system test.

## 3\. Recovery

If the database crashes and then reboots, `LogFile.recover()` will be called before any new transactions start. Your implementation should:

1.  Read the last checkpoint, if any.
2.  Scan forward from the checkpoint (or start of log file, if no checkpoint) to build the set of loser transactions. Redo updates during this pass. You can safely start redo at the checkpoint because `LogFile.logCheckpoint()` flushes all dirty buffers to disk.
3.  Undo the updates of loser transactions.

Design choice: For recovery, the details of your algorithm will be different if you have compensating log records or not. If you do not have compensating log records, be careful about situations where aborted transactions are followed by transactions that update the same pages and successfully commit before the crash.

### Exercise 2: LogFile.recover()

You are now ready to implement LogFile.recover().  After completing this exercise, you should be able to pass all of the `LogTest` system tests.

## 4\. Logistics

You must submit your code (see below) as well as a short (1 page, approximately) writeup describing your approach. This writeup should:

*   Describe your implementation including any design decisions you made. Make sure to emphasize anything that was difficult or unexpected.
*   Discuss and justify any changes you made outside of `LogFile.java`.
*   Describe a unit test that could be added to improve the set of unit tests that we provided for this lab.
*   If you have any feedback for us on the assignment, you can add it to the writeup, send us an email, or make a note and paste your note in the course evaluation form at the end of the quarter.  



### 4.1\. Submitting your assignment

You may submit your code multiple times; we will use the latest version you submit that arrives before the deadline. Place the write-up in a file called `lab4-answers.txt` or `lab4-answers.pdf` in the top level of your repository.

**Important**: In order for your write-up to be added to the git repo, you need to explicitly add it:

```sh
$ git add lab4-answers.txt
```

You also need to explicitly add any other files you create, such as new `*.java` files.

The criteria for your lab being submitted on time is that your code must be tagged and pushed by the due date and time. This means that if one of the TAs or the instructor were to open up GitLab, they would be able to see your solutions on the GitLab web page.

**Just because your code has been committed on your local machine does not mean that it has been submitted -- it needs to be on GitLab!**

There is a bash script `turnInLab.sh` in the root level directory of your repository that commits your changes, deletes any prior tag for the current lab, tags the current commit, and pushes the branch and tag to GitLab. If you are using Linux or Mac OSX, you should be able to run the following:

```sh
$ ./turnInLab.sh lab4
```

You should see something like the following output:

```sh
$ ./turnInLab.sh lab4
[master b155ba0] Lab 4
 1 file changed, 1 insertion(+)
Deleted tag 'lab4' (was b26abd0)
To git@gitlab.com:cse444-20wi/simple-db-pirateninja.git
 - [deleted]         lab3
Counting objects: 11, done.
Delta compression using up to 4 threads.
Compressing objects: 100% (4/4), done.
Writing objects: 100% (6/6), 448 bytes | 0 bytes/s, done.
Total 6 (delta 3), reused 0 (delta 0)
To git@gitlab.com:cse444-22wi/simple-db-pirateninja.git
   ae31bce..b155ba0  master -> master
Counting objects: 1, done.
Writing objects: 100% (1/1), 152 bytes | 0 bytes/s, done.
Total 1 (delta 0), reused 0 (delta 0)
To git@gitlab.com:cse444-22wi/simple-db-pirateninja.git
 * [new tag]         lab4 -> lab4
```

### 4.2\. Submitting a bug

SimpleDB is a relatively complex piece of code. It is very possible you are going to find bugs, inconsistencies, and bad, outdated, or incorrect documentation, etc.

We ask you, therefore, to do this lab with an adventurous mindset. Don't get mad if something is not clear, or even wrong; rather, try to figure it out yourself or send us a friendly email. Please create (friendly!) issues reports on the repository website. When you do, please try to include:

*   A description of the bug.
*   A `.java` file we can drop in the `test/simpledb` directory, compile, and run.
*   A `.txt` file with the data that reproduces the bug. We should be able to convert it to a `.dat` file using `HeapFileEncoder`.

You can also post on the class message board if you feel you have run into a bug.

### 4.3 Grading

80% of your grade will be based on whether or not your code passes the system test suite we will run over it. These tests will be a superset of the tests we have provided; the tests we provide are to help guide your implementation, but they do not define correctness. Before handing in your code, you should make sure produces no errors (passes all of the tests) from both `ant test` and `ant systemtest`.

**Important:** Before testing, we will replace your `build.xml`, `HeapFileEncoder.java`, and the entire contents of the `test/` directory with our version of these files. This means you cannot change the format of the `.dat` files! You should also be careful when changing APIs and make sure that any changes you make are backwards compatible. In other words, during the grading process we will clone your repository, replace the files mentioned above, compile it, and then grade it. It will look roughly like this:

```sh
$ git clone git@gitlab.com:cse444-22wi/simple-db-yourname
$ # [replace build.xml and test]
$ git checkout -- build.xml test\
$ ant test
$ ant systemtest
$ # [additional tests]
```

An additional 20% of your grade will be based on the quality of your writeup and our subjective evaluation of your code.

We've had a lot of fun designing this assignment, and we hope you enjoy hacking on it!
