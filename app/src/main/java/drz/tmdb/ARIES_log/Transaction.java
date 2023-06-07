package drz.tmdb.ARIES_log;

import java.io.*;



/**
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */

public class Transaction {
    private final TransactionId tid;
    volatile boolean started = false;
    public File logFile;//日志文件
    public drz.tmdb.memory.MemManager memManager;
    LogManager1 logManager1=new LogManager1(memManager);

    public Transaction() throws IOException {
        tid = new TransactionId();
    }

    /** Start the transaction running */
    public void start() {
        started = true;
        try {
            LogManager1.logXactionBegin(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public TransactionId getId() {
        return tid;
    }

    /** Finish the transaction */
    public void commit() throws IOException {
        transactionComplete(false);
    }

    /** Finish the transaction */
    public void abort() throws IOException {
        transactionComplete(true);
    }

    /** Handle the details of transaction commit / abort */
    public void transactionComplete(boolean abort) throws IOException {

        if (started) {

            if (abort) {
                LogManager1.logAbort(tid); //does rollback too
            } else {

                Database.getBufferPool().flushPages(tid);
                LogManager1.logCommit(tid);
            }

            try {

                BufferPool.transactionComplete(tid, !abort); // release locks

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }


            started = false;
        }

    }

}
