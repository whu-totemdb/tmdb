package drz.tmdb.sync.share;

import java.io.Serializable;

import drz.tmdb.sync.node.database.Action;

public class WindowEntry implements Serializable {
    private RequestType requestType;

    private Action action;

    public WindowEntry(){}

    public WindowEntry(RequestType requestType, Action action) {
        this.requestType = requestType;
        this.action = action;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public Action getAction() {
        return action;
    }
}
