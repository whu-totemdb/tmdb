package au.edu.rmit.bdm.Torch.base.db;

import au.edu.rmit.bdm.Torch.base.FileSetting;
import edu.whu.tmdb.query.operations.Exception.TMDBException;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;

public class TrajVertexRepresentationPool extends TrajectoryPool {

    public TrajVertexRepresentationPool(boolean isMem, FileSetting setting)  {
        super(isMem, setting);
        tableName = setting.TRAJECTORY_VERTEX_TABLE;
    }
}
