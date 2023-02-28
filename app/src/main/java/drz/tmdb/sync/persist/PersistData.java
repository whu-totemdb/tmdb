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

    public String nodeID;

    public int requestNum;

    public LocalDateTime lastUpdateTime;

    public ConcurrentHashMap<Long, VectorClock> vectorClockMap;

    public DataManager dataManager;

    public SendWindow sendWindow;

    public ConcurrentHashMap<String, NodeInfo> cluster;

    public ArbitrationController arbitrationController;

    public SendInfo sendInfo;

    public ReceiveDataArea receiveDataArea;

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
