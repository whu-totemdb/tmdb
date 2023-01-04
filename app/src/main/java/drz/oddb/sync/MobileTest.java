package drz.oddb.sync;

import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.node.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;

public class MobileTest {


    public static void sync() throws
            IOException {

        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(500),
                Duration.ofMillis(500),
                2
        );



        /*String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);//获取当前机器的IP地址
        //获取当前机器的一个空闲端口
        ServerSocket serverSocket = new ServerSocket(0);
        int receivePort = serverSocket.getLocalPort();

        //InetSocketAddress i = new InetSocketAddress(ip,port);

        System.out.println("ip地址："+ip);
        System.out.println("端口为："+receivePort);

        Node myself = new Node(ip, receivePort, gossipConfig);

        myself.start();*/


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


        for (int i = 1; i <= 5; i++) {
            /*GossipController gossipService = new GossipController
                    (new InetSocketAddress("127.0.0.1", 9090 + i),
                            new InetSocketAddress("127.0.0.1", 9090 + i - 1), gossipConfig);
            gossipService.start(true,initialNode.generateGossipRequest(key));*/
            //InetAddress otherIP = InetAddress.getByName(nodeSelf);
            Node otherNode = new Node(ip, 9090+i, gossipConfig);
            nodeCluster.add(otherNode.getSocketAddress());
            otherNode.start();
            /*initialNode.getGossipController().getNodes().putIfAbsent(otherNode.getNodeID(), otherNode);
            otherNode.getGossipController().getNodes().putIfAbsent(initialNode.getNodeID(), initialNode);*/
            //otherNode.getGossipController().start();
        }

        initialNode.broadcast1(nodeCluster);
        initialNode.updateVectorClock(key);
        //initialNode.getGossipController().setRequest(initialNode.generateGossipRequest(key));





    }
}