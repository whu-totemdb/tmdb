package edu.whu.tmdb.query.operations.impl;

import java.io.IOException;
import java.util.List;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import edu.whu.tmdb.storage.memory.MemManager;
import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.query.operations.utils.traj.TrajTrans;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;

public class TJoinSelect extends SelectImpl{


    private MemConnect memConnect;


    public TJoinSelect() {
        super();
        this.memConnect=MemConnect.getInstance(MemManager.getInstance());
    }

    //TODO TMDB
    //重写select的intersect方法，使其使用trajectory similarity join 进行连接
    @Override
    public SelectResult intersect(SelectResult left, SelectResult right){
        LongestCommonSubSequence longestCommonSubSequence=new LongestCommonSubSequence();
        //新建tuplelist，存储两表intersect之后的结果
        TupleList res=new TupleList();
        //遍历左表的tuple
        for (int i = 0; i < left.getTpl().tuplelist.size(); i++) {
            //获取当前tuple

            //调用TrajTrans的getTraj方法，将tuple中的String轨迹转换成List<Coordinate>的形式，得到traj1
            Tuple tuple = left.getTpl().tuplelist.get(i);
            List<TrajEntry> leftTraj = TrajTrans.getTraj((String) tuple.tuple[2]);
            //遍历右表的每个tuple
            for (int j = 0; j < right.getTpl().tuplelist.size(); j++) {
                //获取当前右表tuple
                //并通过TrajTrans的getTraj方法得到List<Coordinate>，得到traj2
                Tuple rightTuple = right.getTpl().tuplelist.get(j);
                List<TrajEntry> rightTraj = TrajTrans.getTraj((String) rightTuple.tuple[2]);
                //通过longestCommonSubSequence的getCommonSubsequence方法得到traj1和traj2的公共子序列，theta值自设
                List<Coordinate> commonSubsequence = longestCommonSubSequence.getCommonSubsequence(leftTraj, rightTraj, 3);
                //通过得到的子序列的长度设置阈值，判定当前子序列是否值得加入结果集合中
                if(commonSubsequence.size()>=1){
                    //如果满足，则新建加入到结果结合中的tuple
                    //此tuple其它的部分与左表的当前tuple全部一致，除了轨迹段改为公共子序列
                    Tuple temp=new Tuple();
                    temp.tupleId=tuple.tupleId;
                    temp.tupleIds=tuple.tupleIds;
                    temp.tuple=tuple.tuple;
                    //需要将得到的轨迹子序列，转换成string的形式，然后将tuple中轨迹部分设置为转换后的值
                    String temps=TrajTrans.getString(commonSubsequence);
                    temp.tuple[2]=temps;
                    //在新建的tuplelist中加入当前tuple
                    res.tuplelist.add(temp);
                    break;
                }
            }
        }
        //将左表的selectResult 也就是left的tuplelist设置为新的结果集
        left.setTpl(res);
        //返回新的selectrResult
        return left;
    }
}


