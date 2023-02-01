package drz.oddb;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;

import drz.oddb.sync.Sync;
import drz.oddb.sync.config.GossipConfig;
import drz.oddb.sync.node.Node;
import drz.oddb.sync.node.database.Action;
import drz.oddb.sync.node.database.OperationType;
import drz.oddb.sync.statistics.WriteCSV;
import drz.oddb.sync.timeTest.SendTimeTest;
import drz.oddb.sync.util.NetworkUtil;
import drz.oddb.sync.util.NodeUtil;


/**
 * Example local unit test, which will execute on the development machine (host).
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
public class ExampleUnitTest {
    @Test
    public void addition_isCorrect() throws IOException,InterruptedException {
        assertEquals(4, 2 + 2);
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                2
        );

        ArrayList<InetSocketAddress> nodeCluster = new ArrayList<>();

        String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        String nodeID = NodeUtil.obtainNodeID();
        Node initialNode = new Node(nodeID, ip, 9090, gossipConfig);
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
            String otherNodeID = NodeUtil.obtainNodeID();
            Node otherNode = new Node(otherNodeID, ip,9090+i, gossipConfig);
            nodeCluster.add(otherNode.getSocketAddress());
            otherNode.start();
            /*initialNode.getGossipController().getNodes().putIfAbsent(otherNode.getNodeID(), otherNode);
            otherNode.getGossipController().getNodes().putIfAbsent(initialNode.getNodeID(), initialNode);*/
            //otherNode.getGossipController().start();
        }

        initialNode.broadcast1(nodeCluster);

        //insert into test values("a",1,10.0);
        Action action = new Action(
                OperationType.insert,
                "test",
                "test",
                1,
                1,
                3,
                new String[]{"name", "age", "num"},
                new String[]{"String", "Integer", "Double"},
                new String[]{"a", "1", "10.0"});

        for (int m = 1; m <= 3; m++) {

            for (int n = 1; n <= m ; n++){
                //initialNode.updateVectorClock(key);
                initialNode.getDataManager().putAction(action);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            SendTimeTest s = Node.sendTimeTest.get(Node.batch_num);
            s.getProcessQueueAverageTime();
            s.getSendRequestAverageTime();
            s.getWriteObjectAverageTime();
            Node.batch_num++;

        }



        Thread.sleep(10000);

        ArrayList<String[]> sendHead = WriteCSV.getAttributes(new SendTimeTest());
        ArrayList<String[]> sendDataList = new ArrayList<>();
        SendTimeTest test;
        String[] tmp;

        for (Integer i : Node.sendTimeTest.keySet()){
            test = Node.sendTimeTest.get(i);
            tmp = WriteCSV.getAttributeValue(test);
            sendDataList.add(tmp);
        }

        WriteCSV.writeCSVFile("sendTime3",sendHead,sendDataList,false);
        /*ArrayList<String[]> receiveHead = WriteCSV.getAttributes(Node.receiveTimeTest);
        String[] receiveData = WriteCSV.getAttributeValue(Node.receiveTimeTest);
        ArrayList<String[]> receiveDataList = new ArrayList<>();
        receiveDataList.add(receiveData);
        WriteCSV.writeCSVFile("receiveTime",receiveHead,receiveDataList,false);*/
        //while(true){}
        //ArrayList<String[]> data = WriteCSV.getData(new TimeTest());
        //WriteCSV.writeCSVFile("test",data);
    }

    @Test
    public void testBroadcast() throws Exception {
        GossipConfig gossipConfig = new GossipConfig(
                Duration.ofSeconds(3),
                Duration.ofSeconds(3),
                Duration.ofMillis(10),
                Duration.ofMillis(10),
                3
        );


        //String nodeSelf = InetAddress.getLocalHost().getHostAddress();
        String nodeSelf = NetworkUtil.getLocalHostIP();
        InetAddress ip = InetAddress.getByName(nodeSelf);
        System.out.println("IP地址为：" + ip);

        String nodeID = NodeUtil.obtainNodeID();
        Node initialNode = new Node(nodeID, ip, 9091, gossipConfig);

        initialNode.start();



        //initialNode.broadcast();
        try {
            Thread.sleep(10000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        while (true){}


    }

    @Test
    public void testUUID(){
        System.out.println(NodeUtil.obtainNodeID());
    }


}