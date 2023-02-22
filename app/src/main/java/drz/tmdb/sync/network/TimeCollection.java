package drz.tmdb.sync.network;

public class TimeCollection {
    private long requestReceiveTime;

    private long responseSendTime;

    public TimeCollection(long requestReceiveTime, long responseSendTime) {
        this.requestReceiveTime = requestReceiveTime;
        this.responseSendTime = responseSendTime;
    }

    public long getRequestReceiveTime() {
        return requestReceiveTime;
    }

    public long getResponseSendTime() {
        return responseSendTime;
    }
}
