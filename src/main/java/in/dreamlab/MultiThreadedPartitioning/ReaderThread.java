package in.dreamlab.MultiThreadedPartitioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ReaderThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderThread.class);

    private ConcurrentLinkedQueue<Edge> IOQueue;
    private Path file;
    private BufferedReader input;

    private long batchSize = 0;

    // PERF METRIC VARIABLES
    private long totalBatchReadTime = 0L;
    private long totalReaderNotifyingTime = 0L;
    private long totalReaderTime = 0L;
    private long totalIOWaitTime = 0L;

    public ReaderThread(String filePath, long batchSize, ConcurrentLinkedQueue<Edge> IOQueue) {
        this.IOQueue = IOQueue;
        this.batchSize = batchSize;
        try {
            this.file = Paths.get(filePath);
            input = Files.newBufferedReader(this.file);
        } catch (IOException e) {
            LOGGER.info("File not found");
            e.printStackTrace();
        }
    }

    public boolean read(long batchSize) {
        long batchCounter = IOSharedResources.batchCounter.incrementAndGet();
        long readCounter = 0L;
        for(int i = 0; i < batchSize; i++) {
            try {
                String line = input.readLine();
                if (line != null) {
                    Edge e = Edge.toEdge(line);
                    IOQueue.offer(e);
                    readCounter++;
                } else {
                    LOGGER.info("READER.BATCH COUNTER," + batchCounter + ",READER.COUNT OF EDGES READ," + readCounter);
                    IOSharedResources.readCount.addAndGet(readCounter);
                    return false;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        LOGGER.info("READER.BATCH COUNTER," + batchCounter + ",READER.COUNT OF EDGES READ," + readCounter);
        IOSharedResources.readCount.addAndGet(readCounter);
        return true;
    }


    @Override
    public void run() {
        long totalReaderStart = System.currentTimeMillis();
        try {
            IOSharedResources.consumerNotifyLock.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        while (true) {
            try {
                    long readerWaitStart = System.currentTimeMillis();
                    synchronized (IOSharedResources.producerLock) {
                        IOSharedResources.consumerNotifyLock.release();
                        LOGGER.info("Reader waiting for fill request");
                        IOSharedResources.producerLock.wait();
                    }
                    long readerWaitEnd = System.currentTimeMillis();
                    totalIOWaitTime += (readerWaitEnd-readerWaitStart);


                long totalBatchReadStart = System.currentTimeMillis();
                boolean hasMore = read(batchSize);
                long totalBatchReadEnd = System.currentTimeMillis();
                totalBatchReadTime += (totalBatchReadEnd-totalBatchReadStart);

                IOSharedResources.moreLines.set(hasMore);
                IOSharedResources.isReading.compareAndSet(true, false);

                IOSharedResources.consumerNotifyLock.acquire();

                long readerNotifyingStart = System.currentTimeMillis();
                synchronized (IOSharedResources.consumerLock) {
                    LOGGER.info("Reader notifying threads to resume");
//                  IOSharedResources.consumerNotifyLock.release();
                    IOSharedResources.consumerLock.notifyAll();
                }
                long readerNotifyingEnd = System.currentTimeMillis();
                totalReaderNotifyingTime += (readerNotifyingEnd-readerNotifyingStart);


                if (!hasMore) {
                    LOGGER.info("Reader thread exiting");
                    MultiThreadedMaster.readerLatch.countDown();
                    long totalReaderEnd = System.currentTimeMillis();
                    MultiThreadedMaster.totalReaderTime.addAndGet((totalReaderEnd-totalReaderStart));
                    MultiThreadedMaster.reader_totalBatchReadTime.addAndGet(totalBatchReadTime);
                    MultiThreadedMaster.reader_totalNotifyingTime.addAndGet(totalReaderNotifyingTime);
                    MultiThreadedMaster.reader_totalIOWaitTime.addAndGet(totalIOWaitTime);
                    return;
                }
            } catch (InterruptedException e) {
                LOGGER.info("Thread is interrupted");
                e.printStackTrace();
            }
        }
    }
}
