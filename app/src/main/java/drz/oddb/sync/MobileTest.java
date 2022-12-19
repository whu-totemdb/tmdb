package drz.oddb.sync;

import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.node.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;

public class MobileTest {
    public static void main(String[] args) throws
            IOException {

        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(500),
                Duration.ofMillis(500),
                2
        );



        String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);//获取当前机器的IP地址
        //获取当前机器的一个空闲端口
        ServerSocket serverSocket = new ServerSocket(0);
        int receivePort = serverSocket.getLocalPort();

        //InetSocketAddress i = new InetSocketAddress(ip,port);

        System.out.println("ip地址："+ip);
        System.out.println("端口为："+receivePort);

        Node myself = new Node(ip, receivePort, gossipConfig);

        myself.start();



    }
}