package drz.tmdb.sync.network;

import java.net.InetSocketAddress;

public interface GossipUpdater {
    void update(InetSocketAddress inetSocketAddress);
}
