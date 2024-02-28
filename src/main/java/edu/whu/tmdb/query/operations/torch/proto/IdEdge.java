package edu.whu.tmdb.query.operations.torch.proto;/*
 * className:IdEdge
 * Package:edu.whu.tmdb.query.operations.torch.proto
 * Description:
 * @Author: xyl
 * @Create:2023/9/12 - 12:40
 * @Version:v1
 */

public class IdEdge {
    Integer edgeId;
    Integer vertexId1;
    Integer vertexId2;

    public IdEdge(Integer edgeId, Integer vertexId1, Integer vertexId2) {
        this.edgeId = edgeId;
        this.vertexId1 = vertexId1;
        this.vertexId2 = vertexId2;
    }
}
