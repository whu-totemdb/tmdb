package drz.oddb.sync.share;

public class WindowEntry {
    private RequestType requestType;

    private Long key;

    public WindowEntry(){}

    public WindowEntry(RequestType requestType, Long key) {
        this.requestType = requestType;
        this.key = key;
    }

    public Long getKey() {
        return key;
    }
}
