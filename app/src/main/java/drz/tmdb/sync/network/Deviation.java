package drz.tmdb.sync.network;

import java.util.HashMap;

public class Deviation {
    private static long requestSendTime;

    private static long responseReceiveTime;

    //作为接收节点在传输层记录下接收时刻与请求发送时刻并保存在此
    private static long requestReceiveTime;

    private static HashMap<String,TimeCollection> timeCollections = new HashMap<>();

    public static void setRequestSendTime(long requestSendTime) {
        Deviation.requestSendTime = requestSendTime;
    }

    public static long getRequestSendTime() {
        return requestSendTime;
    }

    public static void setResponseReceiveTime(long responseReceiveTime) {
        Deviation.responseReceiveTime = responseReceiveTime;
    }

    public static long getResponseReceiveTime() {
        return responseReceiveTime;
    }

    public static void setRequestReceiveTime(long requestReceiveTime) {
        Deviation.requestReceiveTime = requestReceiveTime;
    }

    public static long getRequestReceiveTime() {
        return requestReceiveTime;
    }

    public static void putTimeCollection(String nodeID, long requestReceiveTime, long responseSendTime){
        timeCollections.put(nodeID,new TimeCollection(requestReceiveTime,responseSendTime));
    }

    //正数表示其他节点时间比本节点本地时间更快，负数表示更慢
    public static long getError(String nodeID){
        TimeCollection timeCollection = timeCollections.get(nodeID);

        long requestReceiveTime = timeCollection.getRequestReceiveTime();
        long responseSendTime = timeCollection.getResponseSendTime();
        long rtt = getRTT(requestSendTime,requestReceiveTime,responseSendTime,responseReceiveTime);
        return requestReceiveTime - (requestSendTime + rtt / 2);
    }

    private static long getRTT(long requestSendTime,long requestReceiveTime,long responseSendTime,long responseReceiveTime){
        return (responseReceiveTime - requestSendTime - (responseSendTime - requestReceiveTime));
    }
}
