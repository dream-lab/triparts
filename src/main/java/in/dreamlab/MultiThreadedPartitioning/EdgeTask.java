package in.dreamlab.MultiThreadedPartitioning;

import gnu.trove.set.hash.TLongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class EdgeTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeTask.class);

    private Edge edge;

    public EdgeTask(Edge edge) {
        this.edge = edge;
    }

    public String getEdge() {
        return edge.toString();
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();

        if (!MultiThreadedWorkerServer.threadIdMap.containsKey(threadId)) {
            Integer op_id = MultiThreadedWorkerServer.threadCount.getAndIncrement();
            LOGGER.info(Thread.currentThread().getName() +  " _ " + threadId + " - op_id - " + op_id);
            MultiThreadedWorkerServer.threadIdMap.put(threadId, op_id);
        }

        byte thread_op_id = MultiThreadedWorkerServer.threadIdMap.get(threadId).byteValue();

        long triangleCountStart = System.currentTimeMillis();
        List<Long> triangleVertices =  MultiThreadedWorker.addEdgeAndCheckTriangles(edge.getSource(), edge.getSink(), thread_op_id);
        long triangleCountEnd = System.currentTimeMillis();

        MultiThreadedWorker.totalTriangleCountTime.addAndGet((triangleCountEnd-triangleCountStart));
        
        long updateTrianglesAddStart = System.currentTimeMillis();
        if (triangleVertices.size() > 0) {
            // acquire the readLock while doing an addAll operation (since this a read operation on the triangle vertices
            MultiThreadedWorkerServer.triSetReadWriteLock.readLock().lock();
            MultiThreadedWorkerServer.triSet.addAll(triangleVertices);
            MultiThreadedWorkerServer.triSetReadWriteLock.readLock().unlock();
        }
        long updateTrianglesAddEnd = System.currentTimeMillis();
        MultiThreadedWorker.totalUpdateTrianglesAddTime.addAndGet(updateTrianglesAddEnd-updateTrianglesAddStart);
        MultiThreadedWorker.totalProcessedEdgeCount.incrementAndGet();
    }
}
