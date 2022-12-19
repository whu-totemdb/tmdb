package drz.oddb.sync.network;

import java.net.InetSocketAddress;

public interface GossipUpdater {
    void update(InetSocketAddress inetSocketAddress);
}
