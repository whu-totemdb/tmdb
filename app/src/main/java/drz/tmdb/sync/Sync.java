package drz.tmdb.sync;



import java.net.InetAddress;

import java.net.UnknownHostException;
import java.time.Duration;


import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.util.NetworkUtil;
import drz.tmdb.sync.util.NodeUtil;

public class Sync {

    private static Node node;

    public static Node getNode() {
        return node;
    }



    public static void initialNode(int receivePort) throws Exception{
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                3
        );

        String nodeID = NodeUtil.obtainNodeID();//后面需要将nodeID持久化保存，如果机器之前生成过则读取保存的值进行node初始化

        String nodeSelf = NetworkUtil.getLocalHostIP();//InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为："+ip);

        node = new Node(nodeID, ip, receivePort, gossipConfig);

        //return initialNode;
    }



    /*public static void start() {
        if(node == null){
            System.out.println("请先初始化节点！");
        }
        else{
            node.start();
        }

    }*/


    public static void broadcast() throws UnknownHostException {
        node.broadcast();
    }

    //对主键为key的数据进行同步操作
    public static void syncStart(Action action){
        //node.updateVectorClock(key);
        node.getDataManager().putAction(action);
        if (action.getOp() != OperationType.select){
            node.getDataManager().putOldAction(action);
        }
    }
}
