package drz.oddb.sync;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.net.wifi.WifiInfo;
import android.net.wifi.WifiManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.node.Node;
import drz.oddb.sync.statistics.WriteCSV;
import drz.oddb.sync.util.NetworkUtil;

public class Sync {

    public static Node initialNode() throws Exception{
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                3
        );



        String nodeSelf = NetworkUtil.getLocalHostIP();//InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为："+ip);

        Node initialNode = new Node(ip, 9090, gossipConfig);

        return initialNode;
    }



    public static void syncStart() throws
            IOException,
            InterruptedException {


    }


    public static void broadcast(Node initialNode) throws UnknownHostException {

        initialNode.broadcast();
    }
}
