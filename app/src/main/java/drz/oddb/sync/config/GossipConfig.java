package drz.oddb.sync.config;

import java.io.Serializable;
import java.time.Duration;

public class GossipConfig implements Serializable {
    public final Duration failureTimeOut;//节点故障超时

    public final Duration cleanUpTimeOut;//超时则移出节点

    public final Duration updateFrequency;//发送频率

    public final Duration failureDetectionFrequency;//故障检测频率

    public final int maxTransmitNode;//转发请求给其他节点的最大数量


    public GossipConfig(Duration failureTimeOut, Duration cleanUpTimeOut, Duration updateFrequency, Duration failureDetectionFrequency, int maxTransmitNode) {
        this.failureTimeOut = failureTimeOut;
        this.cleanUpTimeOut = cleanUpTimeOut;
        this.updateFrequency = updateFrequency;
        this.failureDetectionFrequency = failureDetectionFrequency;
        this.maxTransmitNode = maxTransmitNode;
    }
}