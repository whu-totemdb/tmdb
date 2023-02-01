package drz.tmdb.sync.network;

import java.io.Serializable;
import java.net.InetSocketAddress;

import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.share.ResponseType;
import drz.tmdb.sync.vectorClock.VectorClock;

public class Response implements Serializable {
    private ResponseType responseType;

    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress source;

    private InetSocketAddress target;

    public Response(){}

    public Response(ResponseType responseType, InetSocketAddress source, InetSocketAddress target) {
        this.responseType = responseType;
        this.source = source;
        this.target = target;
    }

    public Response(ResponseType responseType, Action action, VectorClock vectorClock, InetSocketAddress source, InetSocketAddress target) {
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

}
