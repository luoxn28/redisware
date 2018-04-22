package com.luo.redisware.util;

/**
 * @author xiangnan
 * date 2018/4/22
 */
public class ReadMode {

    /**
     * master只读模式，所有读操作都会落到master
     */
    public final static int MASTER_ONLY  = 0;

    /**
     * master_slave读模式，读操作会随机落到master或者slave
     */
    public final static int MASTER_SLAVE = 1;

    /**
     * slave只读模式，读操作都会落到slave
     */
    public final static int SLAVE_ONLY   = 2;

    public static boolean validReadMode(int readMode) {
        return MASTER_ONLY <= readMode && readMode <= SLAVE_ONLY;
    }

}
