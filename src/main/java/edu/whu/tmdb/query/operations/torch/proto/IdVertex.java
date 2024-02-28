package edu.whu.tmdb.query.operations.torch.proto;/*
 * className:IdVertex
 * Package:edu.whu.tmdb.query.operations.torch.proto
 * Description:
 * @Author: xyl
 * @Create:2023/9/12 - 12:40
 * @Version:v1
 */

public class IdVertex {
    int id;
    Double lat;
    Double lng;

    public IdVertex(int id, Double lat, Double lng) {
        this.id = id;
        this.lat = lat;
        this.lng = lng;
    }
}
