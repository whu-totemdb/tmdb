package drz.tmdb.ARIES_log;

import java.io.IOException;



import drz.tmdb.memory.SystemTable.ObjectTableItem;

public class Test {

    public static void test() throws IOException {
        Transaction t1 = new Transaction();

        t1.start();

        // ...模拟其他事物操作
        t1.memManager.add(new ObjectTableItem(0, 0));
        t1.memManager.add(new ObjectTableItem(0, 1));

        // t1结束abort，回滚
        t1.transactionComplete(true);

        // 检查数据库中是否是t1执行前的状态
        t1.memManager.search("t" + 1); // should be null
        // 其他检查
    }
}
