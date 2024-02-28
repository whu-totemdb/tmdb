package edu.whu.tmdb.query.operations.utils.traj;

import java.util.ArrayList;
import java.util.List;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.storage.memory.TupleList;


public class TrajTrans {
    public static String getString(List<Coordinate> list){
        String temps="";
        for (int k = 0; k < list.size()-1; k++) {
            Coordinate coordinate = list.get(k);
            temps+=coordinate.lat+"|"+coordinate.lng+"|";
        }
        temps+=list.get(list.size()-1).lat+"|"
                +list.get(list.size()-1).lng;
        return temps;
    }

    public static List<TrajEntry> getTraj(String s){
        String[] rightSplit= s.split("\\|");
        List<TrajEntry> Traj=new ArrayList<>();
        for (int k = 0; k < rightSplit.length; k+=2) {
            TrajEntry coordinate = new Coordinate(Double.parseDouble(rightSplit[k+1]), Double.parseDouble(rightSplit[k]));
            Traj.add(coordinate);
        }
        return Traj;
    }

    public static String getTorchTraj(String s){
        String[] split = s.split("\\|");
        StringBuilder sb=new StringBuilder("[");
        for (int i = 0; i < split.length-2; i+=2) {
            sb.append("[")
                    .append(split[i])
                    .append(",")
                    .append(split[i+1])
                    .append("]")
                    .append(",");
        }
        sb.append("[")
                .append(split[split.length-2])
                .append(",")
                .append(split[split.length-1])
                .append("]");
        sb.append("]");
        return sb.toString();
    }

    public static String getTmdbTraj(String s){
        return s.replace("[","").replace("]","").replace(",","|");
    }

    public static SelectResult getSelectResultByTrajList(List<Trajectory<TrajEntry>> list, SelectResult selectResult){
        TupleList tupleList=new TupleList();
        for (int i = 0; i < list.size(); i++) {
            List<TrajEntry> trajEntries = list.get(i);
            Tuple tuple=new Tuple();
            tuple.tuple=new Object[]{i,-1,trajEntries.toString()};
            tuple.tupleIds=new int[3];
            tupleList.tuplelist.add(tuple);
        }
        selectResult.setTpl(tupleList);
        return selectResult;
    }

}
