package drz.tmdb.sync;



import java.io.IOException;
import java.net.InetAddress;

import java.net.UnknownHostException;
import java.time.Duration;


import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.persist.Persist;
import drz.tmdb.sync.persist.PersistData;
import drz.tmdb.sync.serializable.Serialization;
import drz.tmdb.sync.util.NetworkUtil;
import drz.tmdb.sync.util.NodeUtil;

public class Sync {

    private static boolean firstInitial = true;

    private static Node node;

    private static String pathName = "D:/tomoDB/app/src/main/java/drz/tmdb/sync/persist/data";

    private static String fileName = "test.txt";

    private static Persist persist = new Persist(pathName,fileName);



    public static Node getNode() {
        return node;
    }



    public static void initialNode(int receivePort) throws Exception{


        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                Duration.ofMillis(5000),
                3,
                2,
                1,
                2,
                65536,
                10
        );
        String nodeSelf = NetworkUtil.getLocalHostIP();//InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为：" + ip);


        if(firstInitial) {
            String nodeID = NodeUtil.obtainNodeID();//后面需要将nodeID持久化保存，如果机器之前生成过则读取保存的值进行node初始化
            node = new Node(nodeID, ip, receivePort, gossipConfig);

            node.start();

            new Thread(() -> {
                PersistData persistData = new PersistData(
                        node.getNodeID(),
                        node.getRequestNum(),
                        node.getLastUpdateTime(),
                        node.getVectorClockMap(),
                        node.getDataManager(),
                        node.getSendWindow(),
                        node.getCluster(),
                        node.getArbitrationController(),
                        node.getGossipController().getSendInfo(),
                        node.getGossipController().getReceiveDataArea()
                );

                byte[] data = Serialization.serialization(persistData);
                persist.write(data);
            }).start();
            firstInitial = false;
            //return initialNode;
        }
        else {
            new Thread(() -> {

                byte[] data = persist.read(Persist.length);
                try {
                    try {
                        PersistData persistData = (PersistData) Serialization.disSerialization(data);

                        node = new Node(
                                ip,
                                receivePort,
                                gossipConfig,
                                persistData.requestNum,
                                persistData.nodeID,
                                persistData.lastUpdateTime,
                                persistData.vectorClockMap,
                                persistData.dataManager,
                                persistData.sendWindow,
                                persistData.cluster,
                                persistData.arbitrationController,
                                persistData.sendInfo,
                                persistData.receiveDataArea
                                );

                        node.start();
                    }catch (ClassNotFoundException e){
                        e.printStackTrace();
                    }

                }catch (IOException e){
                    e.printStackTrace();
                }

            }).start();
        }
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
        /*if (action.getOp() != OperationType.select){
            node.getDataManager().putOldAction(action);
        }*/
    }
}
