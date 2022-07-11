package org.apache.hadoop.security;

import org.junit.Test;

public class TestSnowlake {
    @Test
    public void testGenerator() throws Exception {
        Snowflake snowflake = new Snowflake(1);
        int msCount = 0;
        int tenMsCount = 0;
        int otherCount = 0;
        for (int i = 0; i < 20480000; i ++) {
            long startTime = System.nanoTime();
            snowflake.nextId();
            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            if (totalTime < 500000) {
                msCount += 1;
            } else if (totalTime < 5000000) {
                tenMsCount += 1;
            } else {
                otherCount += 1;
            }
        }
        System.out.println(msCount);
        System.out.println(tenMsCount);
        System.out.println(otherCount);
    }
}
