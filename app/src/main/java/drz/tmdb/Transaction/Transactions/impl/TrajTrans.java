package drz.tmdb.Transaction.Transactions.impl;

import java.util.ArrayList;
import java.util.List;

public class TrajTrans {
    static String getString(List<Coordinate> list){
        String temps="";
        for (int k = 0; k < list.size()-1; k++) {
            Coordinate coordinate = list.get(k);
            temps+=coordinate.lat+"-"+coordinate.lng+"-";
        }
        temps+=list.get(list.size()-1).lat+"-"
                +list.get(list.size()-1).lng;
        return temps;
    }

    static List<Coordinate> getTraj(String s){
        String[] rightSplit= s.split("-");
        List<Coordinate> Traj=new ArrayList<>();
        for (int k = 0; k < rightSplit.length; k+=2) {
            Coordinate coordinate = new Coordinate(Double.parseDouble(rightSplit[k]), Double.parseDouble(rightSplit[k + 1]));
            Traj.add(coordinate);
        }
        return Traj;
    }

}
