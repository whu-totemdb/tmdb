package drz.tmdb.sync.share;

import drz.tmdb.sync.node.database.Action;

public class WindowEntry {
    private RequestType requestType;

    private Action action;

    public WindowEntry(){}

    public WindowEntry(RequestType requestType, Action action) {
        this.requestType = requestType;
        this.action = action;
    }

    public Action getAction() {
        return action;
    }
}
