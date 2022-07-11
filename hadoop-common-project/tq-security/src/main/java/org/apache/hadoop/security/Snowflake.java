package org.apache.hadoop.security;


import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twitter Snowflake
 */
public class Snowflake implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Snowflake.class);

    private static final long serialVersionUID = 1L;

    public static long DEFAULT_TWEPOCH = 1288834974657L;
    public static long DEFAULT_TIME_OFFSET = 2000L;

    private static final long WORKER_ID_BITS = 12L;

    @SuppressWarnings({"PointlessBitwiseExpression", "FieldCanBeLocal"})
    private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);
    private static final long SEQUENCE_BITS = 10L;
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);


    private final long twepoch;
    private final long workerId;

    private final long timeOffset;

    private long sequence = 0L;
    private long lastTimestamp = -1L;
    public static long checkBetween(long value, long min, long max) {
        if (value < min || value > max) {
            throw new IllegalArgumentException("the value " + value + " must be between " + min + " and " + max);
        }

        return value;
    }

    public static long getDefaultId(long maxDatacenterId) {
        if(maxDatacenterId == Long.MAX_VALUE){
            maxDatacenterId -= 1;
        }
        long id = 1L;
        byte[] mac = null;
        try{
            mac = InetAddress.getLocalHost().getAddress();
        } catch (UnknownHostException ignore) {
            LOG.warn("Cannot get local host, unknown host");
        }
        if (null != mac) {
            id = ((0x000000FF & (long) mac[mac.length - 2])
                    | (0x0000FF00 & (((long) mac[mac.length - 1]) << 8))) >> 6;
            id = id % (maxDatacenterId + 1);
        }
        return id;
    }

    public Snowflake(long workerId) {
        this(workerId, DEFAULT_TIME_OFFSET);
    }

    public Snowflake(long workerId,
            long timeOffset) {
        this.twepoch = DEFAULT_TWEPOCH;
        this.workerId = checkBetween(workerId, 0, MAX_WORKER_ID);
        this.timeOffset = timeOffset;
    }

    public long getWorkerId(long id) {
        return id >> WORKER_ID_SHIFT & ~(-1L << WORKER_ID_BITS);
    }

    public long getGenerateDateTime(long id) {
        return (id >> TIMESTAMP_LEFT_SHIFT & ~(-1L << 41L)) + twepoch;
    }

    /**
     * next id
     *
     * @return ID
     */
    public synchronized long nextId() {
        long timestamp = genTime();
        if (timestamp < this.lastTimestamp) {
            if (this.lastTimestamp - timestamp < timeOffset) {
                timestamp = lastTimestamp;
            } else {
                throw new IllegalStateException("Clock moved backwards. Refusing to generate id for "
                        + (lastTimestamp - timestamp));
            }
        }

        if (timestamp == this.lastTimestamp) {
            final long sequence = (this.sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
            this.sequence = sequence;
        }

        this.lastTimestamp = timestamp;
        return ((timestamp - twepoch) << TIMESTAMP_LEFT_SHIFT) | (workerId << WORKER_ID_SHIFT) | sequence;
    }

    public String nextIdStr() {
        return Long.toString(nextId());
    }


    private long tilNextMillis(long lastTimestamp) {
        long timestamp = genTime();
        while (timestamp == lastTimestamp) {
            timestamp = genTime();
        }
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException("Clock moved backwards. Refusing to generate id for "
                    + (lastTimestamp - timestamp));
        }
        return timestamp;
    }

    private long genTime() {
        return System.currentTimeMillis();
    }
}