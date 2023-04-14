package drz.tmdb.map;


import java.text.SimpleDateFormat;
import java.util.Date;

public class TrajectoryPoint {

    double longitude;
    double latitude;
    String timeStr;

    String userId;
    int trajectoryID;

    public TrajectoryPoint(double lo, double la, Date date, String userId){
        this.longitude = lo;
        this.latitude = la;
        this.userId = userId;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.timeStr = df.format(date);

    }

    public TrajectoryPoint(double lo, double la){
        this.longitude = lo;
        this.latitude = la;
    }

}
