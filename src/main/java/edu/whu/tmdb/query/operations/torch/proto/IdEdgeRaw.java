package edu.whu.tmdb.query.operations.torch.proto;/*
 * className:IdEdgeRaw
 * Package:edu.whu.tmdb.query.operations.torch.proto
 * Description:
 * @Author: xyl
 * @Create:2023/9/12 - 12:40
 * @Version:v1
 */

public class IdEdgeRaw {
    int id;
    String lats;
    String lngs;

    public IdEdgeRaw(int id, String lats, String lngs) {
        this.id = id;
        this.lats = lats;
        this.lngs = lngs;
    }
}
