package drz.tmdb.sync.persist;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;

import drz.tmdb.sync.arbitration.ArbitrationController;
import drz.tmdb.sync.node.NodeInfo;
import drz.tmdb.sync.node.database.DataManager;
import drz.tmdb.sync.share.ReceiveDataArea;
import drz.tmdb.sync.share.SendInfo;
import drz.tmdb.sync.share.SendWindow;
import drz.tmdb.sync.vectorClock.VectorClock;

//需要持久化保存的数据
public class PersistData implements Serializable {

    public String nodeID;//节点标识符

    public int requestNum;//节点当前分配的本地请求号

    public LocalDateTime lastUpdateTime;//上一次更新的本地时间

    public ConcurrentHashMap<Long, VectorClock> vectorClockMap;//向量时钟表

    public DataManager dataManager;//同步数据区

    public SendWindow sendWindow;//发送窗口

    public ConcurrentHashMap<String, NodeInfo> cluster;//集群节点信息

    public ArbitrationController arbitrationController;//仲裁管理器

    public SendInfo sendInfo;//当前处理的同步请求的发送数据

    public ReceiveDataArea receiveDataArea;//本节点目前接收的数据区

    public PersistData(
            String nodeID,
            int requestNum,
            LocalDateTime lastUpdateTime,
            ConcurrentHashMap<Long, VectorClock> vectorClockMap,
            DataManager dataManager, SendWindow sendWindow,
            ConcurrentHashMap<String, NodeInfo> cluster,
            ArbitrationController arbitrationController,
            SendInfo sendInfo,
            ReceiveDataArea receiveDataArea) {

        this.nodeID = nodeID;
        this.requestNum = requestNum;
        this.lastUpdateTime = lastUpdateTime;
        this.vectorClockMap = vectorClockMap;
        this.dataManager = dataManager;
        this.sendWindow = sendWindow;
        this.cluster = cluster;
        this.arbitrationController = arbitrationController;
        this.sendInfo = sendInfo;
        this.receiveDataArea = receiveDataArea;
    }
}
