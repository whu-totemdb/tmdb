package drz.oddb.Memory;

import java.util.List;

//磁盘
class PageHeaderData{
    int startFreespace;					//空闲空间开始位置
    int endFreespace;					//空闲空间结束位置
    int tupleNum;						//元组数
    boolean[] flag = new  boolean[1024];	//位图
}

class header{
    int recordLen;
    boolean[] flag = new  boolean[50];		//位图
}


//缓冲区
class buffPointer {
    int blockNum;		//块号
    Boolean flag;		//标记改块是否为脏（true为脏）
    int buf_id;		//缓冲区索引号
}

class ItemPointerData{
    Integer blockNum;
    int offset;			//块内偏移
}
