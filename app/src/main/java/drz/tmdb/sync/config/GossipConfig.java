package drz.tmdb.sync.config;

import java.io.Serializable;
import java.time.Duration;

public class GossipConfig implements Serializable {
    public final Duration failureTimeOut;//节点故障超时

    public final Duration cleanUpTimeOut;//超时则移出节点

    public final Duration updateFrequency;//发送频率

    public final Duration failureDetectionFrequency;//故障检测频率

    public final Duration broadcastFrequency;

    public final int maxTransmitNode;//转发请求给其他节点的最大数量

    public final int copyNum;

    public final int minWriteNum;

    public final int minReadNum;

    public final int maxSendNum;

    public final int windowSize;


    public GossipConfig(Duration failureTimeOut,
                        Duration cleanUpTimeOut,
                        Duration updateFrequency,
                        Duration failureDetectionFrequency,
                        Duration broadcastFrequency,
                        int maxTransmitNode,
                        int copyNum,
                        int minWriteNum,
                        int minReadNum,
                        int maxSendNum,
                        int windowSize) {
        this.failureTimeOut = failureTimeOut;
        this.cleanUpTimeOut = cleanUpTimeOut;
        this.updateFrequency = updateFrequency;
        this.failureDetectionFrequency = failureDetectionFrequency;
        this.broadcastFrequency = broadcastFrequency;
        this.maxTransmitNode = maxTransmitNode;
        this.copyNum = copyNum;
        this.minWriteNum = minWriteNum;
        this.minReadNum = minReadNum;
        this.maxSendNum = maxSendNum;
        this.windowSize = windowSize;
    }
}