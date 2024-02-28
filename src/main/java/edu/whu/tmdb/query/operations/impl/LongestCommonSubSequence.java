package edu.whu.tmdb.query.operations.impl;


import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;



public class LongestCommonSubSequence {
    static Comparator<TrajEntry> comparator = (p1, p2) -> {
        double dist = Geo.distance((Coordinate) p1, (Coordinate) p2);
        if (dist <= 50) return 0;
        return 1;
    };
    public List<Coordinate> getCommonSubsequence(List<TrajEntry> firstTrajectory, List<TrajEntry> secondTrajectory, int theta) {
        int m = firstTrajectory.size();
        int n = secondTrajectory.size();
        int[][] dp = new int[firstTrajectory.size() + 1][secondTrajectory.size() + 1];

        // Calculate the LCSS matrix
        for (int i = 1; i <= firstTrajectory.size(); i++) {
            for (int j = 1; j <= secondTrajectory.size(); j++) {
                if (Math.abs(i - j) <= theta) {
                    if (comparator.compare(firstTrajectory.get(i - 1), secondTrajectory.get(j - 1)) == 0) {
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                    } else {
                        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                    }
                }
            }
        }
        List<Coordinate> result = new ArrayList<>();
        int i = m, j = n;
        int c=dp[m][n];
//        System.out.println(comparator.compare(firstTrajectory.get(10),secondTrajectory.get(9))==0);
        while (i > 0 && j > 0 && c >0 ) {
            if (comparator.compare(firstTrajectory.get(i - 1),secondTrajectory.get(j - 1))==0) {
                result.add((Coordinate) firstTrajectory.get(i - 1));
                c--;
                i--;
                j--;
            } else if (dp[i - 1][j] > dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }
        Collections.reverse(result);
        return result;
    }



}

class Geo{

    public static double distance(Coordinate v1, Coordinate v2){
        return distance(v1.getLat(), v2.getLat(), v1.getLng(), v2.getLng());
    }

    /**
     * Calculate geo-distance between two points in latitude and longitude.
     * <p>
     *
     * @param lat1 latitude of point1 GPS position
     * @param lat2 latitude of point2 GPS position
     * @param lon1 longitude of point1 GPS position
     * @param lon2 longitude of point2 GPS position
     * @return Distance in Meters
     */
    public static double distance(double lat1, double lat2, double lon1, double lon2) {
        return distance(lat1, lat2, lon1, lon2, 0.0, 0.0);
    }

    /**
     * Calculate geo-distance between two points in latitude and longitude taking
     * into account height difference. Uses Haversine method as its base.
     * <p>
     * lat1, lon1 Start candidatePoint lat2, lon2 End candidatePoint el1 Start altitude in meters
     * el2 End altitude in meters
     *
     * @return Distance in Meters
     */

    private static double distance(double lat1, double lat2, double lon1, double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }

}
//class Coordinate{
//
//    public final double lat;
//    public final double lng;
//
//    public Coordinate(double lat, double lng){
//        this.lat = lat;
//        this.lng = lng;
//    }
//
//    public int getId() {
//        return -1;
//    }
//
//    public double getLat(){
//        return lat;
//    }
//
//    public double getLng(){
//        return lng;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        return hashCode() == o.hashCode();
//    }
//
//    @Override
//    public int hashCode() {
//        int result;
//        long temp;
//        temp = Double.doubleToLongBits(this.lat);
//        result = (int) (temp ^ (temp >>> 32));
//        temp = Double.doubleToLongBits(lng);
//        result = 31 * result + (int) (temp ^ (temp >>> 32));
//        return result;
//    }
//
//    @Override
//    public String toString() {
//        return "{" + this.lat + ", " + lng + '}';
//    }
//}
