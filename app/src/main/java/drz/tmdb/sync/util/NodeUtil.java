package drz.tmdb.sync.util;

import java.util.UUID;

public class NodeUtil {

    public static String obtainNodeID(){
        String nodeID;

        nodeID = UUID.randomUUID().toString().replaceAll("-", "");

        return nodeID;

    }
}
