package drz.tmdb.sync.share;

import java.io.Serializable;

import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.node.database.Action;

public class WindowEntry implements Serializable {
    private RequestType requestType;

    private Action action;

    private GossipRequest gossipRequest;

    public WindowEntry(){}

    public WindowEntry(RequestType requestType, Action action) {
        this.requestType = requestType;
        this.action = action;
    }

    public WindowEntry(RequestType requestType, GossipRequest gossipRequest) {
        this.requestType = requestType;
        this.gossipRequest = gossipRequest;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public Action getAction() {
        return action;
    }

    public GossipRequest getGossipRequest() {
        return gossipRequest;
    }
}
