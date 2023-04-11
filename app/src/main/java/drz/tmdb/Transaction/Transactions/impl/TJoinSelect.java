package drz.tmdb.Transaction.Transactions.impl;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.ArrayList;
import java.util.List;


import drz.tmdb.Memory.SystemTable.ClassTableItem;
import drz.tmdb.Memory.Tuple;
import drz.tmdb.Memory.TupleList;
import drz.tmdb.Transaction.Transactions.Exception.TMDBException;
import drz.tmdb.Transaction.Transactions.utils.MemConnect;
import drz.tmdb.Transaction.Transactions.utils.SelectResult;

//TODO TMDB
public class TJoinSelect extends SelectImpl{


    private MemConnect memConnect;

    public TJoinSelect(MemConnect memConnect) {
        super(memConnect);
        this.memConnect = memConnect;
    }

    public TJoinSelect() {

    }

    //TODO TMDB
    //重写select的intersect方法，使其使用trajectory similarity join 进行连接
//    @Override
//    public SelectResult intersect(SelectResult left, SelectResult right){
        //新建tuplelist，存储两表intersect之后的结果

        //遍历左表的tuple

            //获取当前tuple

            //调用TrajTrans的getTraj方法，将tuple中的String轨迹转换成List<Coordinate>的形式，得到traj1


            //遍历右表的每个tuple

                //获取当前右表tuple
                //并通过TrajTrans的getTraj方法得到List<Coordinate>，得到traj2


                //通过longestCommonSubSequence的getCommonSubsequence方法得到traj1和traj2的公共子序列，theta值自设

                //通过得到的子序列的长度设置阈值，判定当前子序列是否值得加入结果集合中

                    //如果满足，则新建加入到结果结合中的tuple
                    //此tuple其它的部分与左表的当前tuple全部一致，除了轨迹段改为公共子序列




                    //需要将得到的轨迹子序列，转换成string的形式，然后将tuple中轨迹部分设置为转换后的值


                    //在新建的tuplelist中加入当前tuple





        //将左表的selectResult 也就是left的tuplelist设置为新的结果集

        //返回新的selectrResult

//    }
}


