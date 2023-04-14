package drz.tmdb.sync;



import android.content.Context;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;


import drz.tmdb.sync.config.GossipConfig;
import drz.tmdb.sync.node.Node;
import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.node.database.OperationType;
import drz.tmdb.sync.persist.Persist;
import drz.tmdb.sync.persist.PersistData;
import drz.tmdb.sync.serializable.Serialization;
import drz.tmdb.sync.util.FilePathUtil;
import drz.tmdb.sync.util.NetworkUtil;
import drz.tmdb.sync.util.NodeUtil;

public class Sync {

    private static boolean firstInitial = true;

    private static Node node;

    private static String pathName /*= "/data/user/0/drz.tmdb/files"; "D:/tomoDB/app/src/main/java/drz/tmdb/sync/persist/data"*/;

    private static String fileName = "persist_data.dat";

    private static Persist persist /*= new Persist(pathName,fileName)*/;

    private static Context context;



    public static Node getNode() {
        return node;
    }

    public static void setPathName(String pathName) {
        Sync.pathName = pathName;
    }

    public static void setContext(Context context) {
        Sync.context = context;
    }

    public static Context getContext() {
        return context;
    }



    public static void initialNode(int receivePort, Context context) throws Exception{
        Sync.context = context;

        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                Duration.ofMillis(5000),
                3,
                3,
                2,
                2,
                65536,
                10
        );
        String nodeSelf = NetworkUtil.getLocalHostIP();//InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为：" + ip);
        if (pathName==null){
            pathName = FilePathUtil.getFileDir(context);
            persist = new Persist(pathName,fileName);
        }

        if(persist==null){
            persist = new Persist(pathName,fileName);
        }

        File file = new File(pathName,fileName);

        if(firstInitial || !file.exists()) {
            String nodeID = NodeUtil.obtainNodeID();
            node = new Node(nodeID, ip, receivePort, gossipConfig);

            node.start();

            persistData();
            firstInitial = false;
            //return initialNode;
        }
        else {
            byte[] data = persist.read(Persist.length);
            try {
                try {
                    PersistData persistData = (PersistData) Serialization.deSerialization(data);

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

        }
    }

    public static void initialNode(int receivePort) throws Exception{
        persist = new Persist("D:/tomoDB/app/src/main/java/drz/tmdb/sync/persist/data",fileName);

        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                Duration.ofMillis(5000),
                3,
                3,
                2,
                2,
                65536,
                10
        );
        String nodeSelf = NetworkUtil.getLocalHostIP();//InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为：" + ip);

        File file = new File("D:/tomoDB/app/src/main/java/drz/tmdb/sync/persist/data",fileName);

        if(firstInitial || !file.exists()) {
            String nodeID = NodeUtil.obtainNodeID();
            node = new Node(nodeID, ip, receivePort, gossipConfig);

            node.start();

            persistData();
            firstInitial = false;
            //return initialNode;
        }
        else {
            byte[] data = persist.read(Persist.length);
            try {
                try {
                    PersistData persistData = (PersistData) Serialization.deSerialization(data);

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

    public static void persistData(){
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
    }

}
