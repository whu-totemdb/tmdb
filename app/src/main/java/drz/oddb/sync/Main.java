package drz.oddb.sync;

import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.network.GossipController;
import drz.oddb.sync.node.Node;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws UnknownHostException{

        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(500),
                Duration.ofMillis(500),
                3
        );

        /*GossipController initialNode = new GossipController(new InetSocketAddress("127.0.0.1", 9090), gossipConfig);

        initialNode.setOnNewMember((inetSocketAddress) -> {
            System.out.println("Connected to " +
                    inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort());
        });

        initialNode.setOnFailedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " failed");
        });

        initialNode.setOnRemovedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " removed");
        });

        initialNode.setOnRevivedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " revived");
        });*/

        ArrayList<InetSocketAddress> nodeCluster = new ArrayList<>();

        String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        Node initialNode = new Node(ip, 9090, gossipConfig);
        nodeCluster.add(initialNode.getSocketAddress());
        initialNode.start();

        initialNode.getGossipController().setOnNewMember((inetSocketAddress) -> {
            System.out.println("Connected to " +
                    inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort());
        });

        initialNode.getGossipController().setOnFailedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " failed");
        });

        initialNode.getGossipController().setOnRemovedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " removed");
        });

        initialNode.getGossipController().setOnRevivedMember((inetSocketAddress) -> {
            System.out.println("Node " + inetSocketAddress.getHostName() + ":"
                    + inetSocketAddress.getPort() + " revived");
        });


        final Long key = 1L;



        //initialNode.getGossipController().start();


        for (int i = 1; i <= 3; i++) {
            /*GossipController gossipService = new GossipController
                    (new InetSocketAddress("127.0.0.1", 9090 + i),
                            new InetSocketAddress("127.0.0.1", 9090 + i - 1), gossipConfig);
            gossipService.start(true,initialNode.generateGossipRequest(key));*/
            //InetAddress otherIP = InetAddress.getByName(nodeSelf);
            Node otherNode = new Node(ip, 9090 + i,  gossipConfig);
            nodeCluster.add(otherNode.getSocketAddress());
            otherNode.start();
            /*initialNode.getGossipController().getNodes().putIfAbsent(otherNode.getNodeID(), otherNode);
            otherNode.getGossipController().getNodes().putIfAbsent(initialNode.getNodeID(), initialNode);*/
            //otherNode.getGossipController().start();
        }

        initialNode.broadcast(nodeCluster);
        initialNode.updateVectorClock(key);
        //initialNode.getGossipController().setRequest(initialNode.generateGossipRequest(key));
    }
}