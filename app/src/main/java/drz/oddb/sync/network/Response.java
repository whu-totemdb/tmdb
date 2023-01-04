package drz.oddb.sync.network;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Response implements Serializable {
    private InetSocketAddress source;

    private InetSocketAddress target;

    private boolean broadcast;

    public Response(){}

    public Response(InetSocketAddress source, InetSocketAddress target, boolean broadcast) {
        this.source = source;
        this.target = target;
        this.broadcast = broadcast;
    }

    public InetSocketAddress getSource() {
        return source;
    }

    public InetSocketAddress getTarget() {
        return target;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }
}
