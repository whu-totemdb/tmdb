package edu.whu.tmdb.query.operations.torch.proto;/*
 * className:TrajectoryTimePartial
 * Package:edu.whu.tmdb.query.operations.torch.proto
 * Description:
 * @Author: xyl
 * @Create:2023/9/12 - 12:40
 * @Version:v1
 */

public class TrajectoryTimePartial {
    String id;
    String start;
    String end;

    public TrajectoryTimePartial(String id, String start, String end) {
        this.id = id;
        this.start = start;
        this.end = end;
    }
}
