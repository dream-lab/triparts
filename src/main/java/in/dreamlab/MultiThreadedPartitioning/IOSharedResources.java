package in.dreamlab.MultiThreadedPartitioning;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IOSharedResources {
    protected static ConcurrentLinkedQueue<Edge> IOQueue = new ConcurrentLinkedQueue<>();
    protected static Semaphore consumerNotifyLock = new Semaphore(1);
    protected static Object producerLock = new Object();
    protected static Object consumerLock = new Object();
    protected static AtomicBoolean moreLines = new AtomicBoolean(true);
    protected static AtomicLong readCount = new AtomicLong(0);
    protected static AtomicLong consumedCount = new AtomicLong(0);
    protected static AtomicLong batchCounter = new AtomicLong(0);
    protected static AtomicBoolean isReading = new AtomicBoolean(false);
}
