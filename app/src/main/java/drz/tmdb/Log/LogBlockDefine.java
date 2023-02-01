package drz.tmdb.Log;

class LogBlockHeader {  //事务块空闲空间开始位置
        int startFreespace;
}
class LogPointerData {  //可能没用
        Integer LogBlockNum;
        int offset;
}
