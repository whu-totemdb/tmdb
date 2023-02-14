package drz.tmdb.sync.network;

import java.io.Serializable;
import java.net.InetSocketAddress;

import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.share.ResponseType;
import drz.tmdb.sync.vectorClock.VectorClock;

public class Response implements Serializable {
    private ResponseType responseType;

    private String requestID;

    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress source;

    private InetSocketAddress target;

    public Response(){}

    public Response(String requestID, ResponseType responseType, InetSocketAddress source, InetSocketAddress target) {
        this.requestID = requestID;
        this.responseType = responseType;
        this.source = source;
        this.target = target;
    }

    public Response(String requestID, ResponseType responseType, Action action, VectorClock vectorClock, InetSocketAddress source, InetSocketAddress target) {
        this.requestID = requestID;
        this.responseType = responseType;
        this.action = action;
        this.vectorClock = vectorClock;
        this.source = source;
        this.target = target;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public InetSocketAddress getSource() {
        return source;
    }

    public InetSocketAddress getTarget() {
        return target;
    }


    public String getRequestID() {
        return requestID;
    }

    public Action getAction() {
        return action;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }
}
