package drz.oddb.Level;


import java.util.Map;

import drz.oddb.Transaction.Constant;

// 用于compaction时的排序
public class FileInfo implements Comparable {

    int fileSuffix;
    int size;
    String minKey;
    String maxKey;

    // constructor, 用于测试
    public FileInfo(int fileSuffix, int size, String minKey, String maxKey) {
        this.fileSuffix = fileSuffix;
        this.size = size;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    // construtor，传入fileSuffix
    FileInfo(int fileSuffix, Map<String, String> totalIndex){
        this.fileSuffix = fileSuffix;

        // 去totalIndex获取minKey和maxKey
        String info = totalIndex.get("" + fileSuffix);
        String[] t = info.split("-");
        this.size = Integer.parseInt(t[1]);
        this.minKey = t[2];
        this.maxKey = t[3];
    }


    // 根据minKey排序
    @Override
    public int compareTo(Object o) {
        if(o instanceof FileInfo){
            FileInfo f2 = (FileInfo) o;
            if(!this.minKey.equals(f2.minKey))
                return this.minKey.compareTo(f2.minKey);
            // 如果minKey相等，把大的文件排序在前面，便于compaction
            if(this.size < f2.size)
                return -1;
            else
                return 1;
        }
        return 0;
    }


    // 判断f1和f2的key范围是否互相包含
    public boolean contains(FileInfo f2){
        // 由于已经排好序了，f1.minKey一定小于f2.minKey因此只需要比较这一步
        if(this.maxKey.compareTo(f2.maxKey) > 0)
            return true;
        else
            return false;
    }

    public boolean hasOverlap(FileInfo f2){
        return Constant.hasOverlap(this.minKey, this.maxKey, f2.minKey, f2.maxKey);
    }
}
