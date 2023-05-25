package drz.tmdb.ARIES_log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;


//用于读取每条log记录
//根据不同的record，读取不同的raf长度
public class LogRecord {
    protected long tid;
    protected long offset;

    public LogRecord(RandomAccessFile raf) throws IOException {
        tid = raf.readLong();
    }

    public long getTid()
    {
        return tid;
    }

    public long getOffset()
    {
        return offset;
    }

    public static LogRecord readNext(RandomAccessFile raf) throws IOException
    {
        int record = raf.readInt();
        switch(record)
        {
            case LogManager1.ABORT_RECORD:
                return new AbortRecord(raf);
            case LogManager1.COMMIT_RECORD:
                return new CommitRecord(raf);
            case LogManager1.UPDATE_RECORD:
                return new UpdateRecord(raf);
            case LogManager1.BEGIN_RECORD:
                return new BeginRecord(raf);
            case LogManager1.CHECKPOINT_RECORD:
                return new CheckPointRecord(raf);
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return "LogRecord{" +
                "tid=" + tid +
                ", offset=" + offset +
                '}';
    }
}

class AbortRecord extends LogRecord{

    public AbortRecord(RandomAccessFile raf) throws IOException {
        super(raf);
        offset = raf.readLong();
    }

    @Override
    public String toString() {
        return "AbortRecord{" +
                "tid=" + tid +
                ", offset=" + offset +
                '}';
    }
}

class CommitRecord extends LogRecord{
    public CommitRecord(RandomAccessFile raf) throws IOException {
        super(raf);
        offset = raf.readLong();
    }

    @Override
    public String toString() {
        return "CommitRecord{" +
                "tid=" + tid +
                ", offset=" + offset +
                '}';
    }
}

class BeginRecord extends LogRecord{

    public BeginRecord(RandomAccessFile raf) throws IOException {
        super(raf);
        offset = raf.readLong();
    }

    @Override
    public String toString() {
        return "BeginRecord{" +
                "tid=" + tid +
                ", offset=" + offset +
                '}';
    }
}

class UpdateRecord extends LogRecord{

    private final Entry before;
    private final Entry after;

    public UpdateRecord(RandomAccessFile raf) throws IOException {
        super(raf);
        before = Database.getLogFile().readPageData(raf);
        after = Database.getLogFile().readPageData(raf);
        offset = raf.readLong();
    }

    public Entry getBefore()
    {
        return before;
    }

    public Entry getAfter()
    {
        return after;
    }

    @Override
    public String toString() {
        return "CheckPointRecord{" +
                "tid=" + tid +
                ", offset=" + offset +
                '}';
    }
}

class CheckPointRecord extends LogRecord{

    private final HashMap<Long, Long> tidToFirstLogRecord;

    public CheckPointRecord(RandomAccessFile raf) throws IOException {
        super(raf);
        int keySize = raf.readInt();
        tidToFirstLogRecord = new HashMap<>();
        for(int i=0; i<keySize; ++i)
        {
            tidToFirstLogRecord.put(raf.readLong(), raf.readLong());
        }
        offset = raf.readLong();
    }

    public HashMap<Long, Long> getTidToFirstLogRecord()
    {
        return tidToFirstLogRecord;
    }

    @Override
    public String toString()
    {
        return "CheckPointRecord{" +
                "offset=" + offset +
                '}';
    }
}
