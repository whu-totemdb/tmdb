package drz.oddb.sync.node;

public class QueueEntry {


    private RequestType requestType;

    private Long key;

    public QueueEntry(){}

    public QueueEntry(RequestType requestType, Long key) {
        this.requestType = requestType;
        this.key = key;
    }
}
