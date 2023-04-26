package drz.tmdb.memory;

public class Constant {

    // 数据库系统表文件目录
    public static final String SYSTEM_TABLE_DIR = "/data/data/drz.tmdb/sys/";

    // memTable最大大小为4MB=4*1024*1024B，超过就会触发compact到外存
    public static final long MAX_MEM_SIZE = 4L * 1024 * 1024;
}
