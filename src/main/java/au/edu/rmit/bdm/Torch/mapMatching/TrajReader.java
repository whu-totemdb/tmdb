package au.edu.rmit.bdm.Torch.mapMatching;

import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.base.model.TrajNode;
import au.edu.rmit.bdm.Torch.base.model.Trajectory;
import edu.whu.tmdb.storage.memory.Tuple;
import edu.whu.tmdb.query.Transaction;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.query.operations.utils.traj.TrajTrans;
import net.sf.jsqlparser.JSQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static edu.whu.tmdb.util.FileOperation.getFileNameWithoutExtension;

/**
 * The class is for reading trajectories following the certain format from file.
 */
public class TrajReader {

    private final int BATCH_SIZE;
    private static Logger logger = LoggerFactory.getLogger(TrajReader.class);
    private static LineNumberReader trajReader;
    private static BufferedReader dateReader = null;

    public TrajReader(){
        BATCH_SIZE = 100000;
    }

    public TrajReader(MMProperties props){
        BATCH_SIZE = props.batchSize;
    }

    /**
     * Read raw trajectories.
     * Trajectories that do not follow the format or contain illegal data will be discarded.
     *
     * @param trajSrcPath File containing trajectories.
     * @param dateDataPath File containing timestamp of nodes in trajectories.
     *                 This file could be null and if this is the case, the program will leave time field in trajectory model blank.
     * @return a list of trajectories.
     */
    public boolean readBatch(String trajSrcPath, File dateDataPath, List<Trajectory<TrajEntry>> trajectoryList) {

        logger.info("now reading trajectories");

        trajectoryList.clear();
        boolean hasDate = (dateDataPath != null);
        boolean finished = false;
        SimpleDateFormat sdfmt = null;
        if (hasDate) sdfmt = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Transaction transaction = Transaction.getInstance();
        SelectResult query=null;
        try {
            query = transaction.query("select * from traj" +
                    " where traj_name='" + getFileNameWithoutExtension(trajSrcPath) + "';");
        }catch (JSQLParserException e){
            logger.warn(e.getMessage());
        }
        for (int i = 0; i < query.getTpl().tuplelist.size(); i++) {
            Tuple tuple = query.getTpl().tuplelist.get(i);
            if (i+1 % BATCH_SIZE == 0) {
                logger.info("have readBatch {} trajectories in total", i);
                return false;
            }
            Trajectory<TrajEntry> trajectory = new Trajectory<>((String)tuple.tuple[0], false);
            List<TrajEntry> traj = TrajTrans.getTraj((String) tuple.tuple[3]);
            for (int j = 0; j < traj.size(); j++) {
                trajectory.add(new TrajNode(traj.get(j).getLat(),traj.get(j).getLng()));
            }
            trajectoryList.add(trajectory);
        }
        return true;
//        try {
//            if (trajReader == null)
//                trajReader = new LineNumberReader(new FileReader(trajSrcPath));
//            if (hasDate) dateReader = new BufferedReader(new FileReader(dateDataPath));
//
//            String trajLine = null;
//            String dateLine = null;
//
//            while (true) {
//
//                if ((trajLine = trajReader.readLine()) == null){
//                    trajReader.close();
//                    if (hasDate) dateReader.close();
//                    return true;
//                }
//
//                if (hasDate) dateLine = dateReader.readLine();
//
//                String[] temp = trajLine.split("\t");
//                String trajId = temp[0];
//                String trajContent = temp[1];
//                String dateContent = null;
//
//                try {
//                    trajContent = trajContent.substring(2, trajContent.length() - 2); //remove head "[[" and tail "]]"
//                    if (hasDate) {
//                        dateContent = dateLine.split("\t")[1];
//                        dateContent = dateContent.substring(1, dateContent.length() - 1); //remove head "[" and tail "]"
//                    }
//
//                }catch (Exception e){
//                    logger.warn("trajectory id {} is excluded, either too short or contain illegal chars", trajId);
//                    continue;
//                }
//
//                String[] trajTuples = trajContent.split("],\\[");
//                String[] dateTuples = null;
//
//                if (dateContent != null) {
//                    dateTuples = dateContent.split(",");
//                    assert trajTuples.length == dateTuples.length;
//                }
//
//                Trajectory<TrajEntry> trajectory = new Trajectory<>(trajId, hasDate);
//
//                String[] latLng;
//                for (int i = 0; i < trajTuples.length; i++){
//
//                    double lat = 0.;
//                    double lon = 0.;
//                    try {
//                        latLng = trajTuples[i].split(",");
//                        lat = Double.parseDouble(latLng[1]);
//                        lon = Double.parseDouble(latLng[0]);
//                    }catch (Exception e){
//                        logger.warn("node of trajectory ID {} contains illegal gps location, that node will be excluded. " + trajId);
//                        continue;
//                    }
//
//                    TrajNode node = new TrajNode(lat, lon);
//
//                    if (hasDate) {
//                        Date date;
//                        try {
//                            date = sdfmt.parse(dateTuples[i]);
//                        } catch (ParseException e) {
//                            logger.warn("node of trajectory ID {} contains illegal timestamp, that node will be excluded. " + trajId);
//                            continue;
//                        }
//                        node.setTime(date.getTime());
//                    }
//
//                    trajectory.add(node);
//                }
//                trajectoryList.add(trajectory);
//
//                if (trajReader.getLineNumber() % BATCH_SIZE == 0) {
//                    logger.info("have readBatch {} trajectories in total", trajReader.getLineNumber());
//                    return false;
//                }
//            }
//
//        } catch (IOException e) {
//            throw new IllegalStateException(e.getMessage());
//        }
    }
}
