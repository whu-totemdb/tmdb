package drz.tmdb.sync.timer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import drz.tmdb.sync.network.GossipRequest;

public class SendData {
    public GossipRequest gossipRequest;

    public HashMap<InetSocketAddress,Boolean> responseReceived = new HashMap<>();

    public HashMap<InetSocketAddress,MyTimer> timers = new HashMap<>();

    public MyTimer timer;

    public SendData(GossipRequest gossipRequest, List<InetSocketAddress> otherNodesToSend) {
        this.gossipRequest = gossipRequest;

        for (InetSocketAddress socketAddress : otherNodesToSend){
            responseReceived.put(socketAddress,false);
        }
    }

    public ArrayList<InetSocketAddress> getNotReceivedAddresses(){
        ArrayList<InetSocketAddress> result = new ArrayList<>();

        for (InetSocketAddress socketAddress : responseReceived.keySet()){
            if (!responseReceived.get(socketAddress)){
                result.add(socketAddress);
            }
        }

        return result;
    }
}
