package in.dreamlab.MultiThreadedPartitioning;

import in.dreamlab.MultiThreadedPartitioning.thrift.MessageType;
import in.dreamlab.MultiThreadedPartitioning.thrift.MultiThreadedWorkerService;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiThreadedEdgeTransporter implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedEdgeTransporter.class);
    private MultiThreadedWorkerService.Client workerClient;
    private MultiThreadedWorkerService.AsyncClient asyncWorkerClient;
    private ConcurrentLinkedQueue<Edge> transportQueue;
    protected static AtomicBoolean exitFlag = new AtomicBoolean(false);
    private Edge lastSentEdge;
    private int partition;

    // PERF METRIC VARIABLES
    private long totalQueueReadTime = 0L;
    private long totalRpcTime = 0L;
    private long totalTransportTime = 0L;
    private long totalCheckAndTransport = 0L;

    public MultiThreadedEdgeTransporter(int partitionID, ConcurrentLinkedQueue<Edge> transportQueue) {
        this.partition = partitionID;
        this.transportQueue = transportQueue;
        this.workerClient = MultiThreadedMaster.workerClientMap.get(partition);
        this.asyncWorkerClient = MultiThreadedMaster.asyncWorkerClientMap.get(partition);
    }

    private void sendSingleEdgeToPartition(long v1, long v2, byte edgeColor) {
        try {
            workerClient.singleEdgeReceiver(MessageType.EDGE, v1, v2, edgeColor);
            MultiThreadedMaster.edgeCountDownTransporter.decrementAndGet();
            MultiThreadedMaster.transporterSentCount.incrementAndGet();
            if (MultiThreadedMaster.edgeCountDownTransporter.get() == 0) {
                exitFlag.set(true);
            }

        } catch (TException e) {
            LOGGER.error("TIME_MS," + System.currentTimeMillis() + ",ERROR,SEND_EDGE,IP," +
                    MultiThreadedMaster.workerInfo.get(partition).getIP() + ",PORT," +
                    MultiThreadedMaster.workerInfo.get(partition).getPort());
            e.printStackTrace();
        }
    }

    public void transportEdge() {
        while (true) {
            long queueReadStart = System.currentTimeMillis();
            Edge edge = transportQueue.poll();
            long queueReadEnd = System.currentTimeMillis();
            MultiThreadedMaster.total_transport_queue_read_time.addAndGet((queueReadEnd-queueReadStart));

            if (edge != null) {
                byte green = 0;
                long rpc_start = System.currentTimeMillis();
                sendSingleEdgeToPartition(edge.getSource(), edge.getSink(), green);
                long rpc_end = System.currentTimeMillis();
                MultiThreadedMaster.transport_totalSendTime.addAndGet((rpc_end-rpc_start));
                lastSentEdge = edge;
            } else {
                if (exitFlag.get()) {
                    LOGGER.info("Edge Countdown - " + MultiThreadedMaster.edgeCountDownTransporter.get());
                    break;
                }
                else if (MultiThreadedMaster.updateRequired.get()) {
                }
            }
        }
    }


    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        LOGGER.info( " Transporter " + Thread.currentThread().getName() + " starting..");
        long transportStart = System.currentTimeMillis();
        transportEdge();

        long transportEnd = System.currentTimeMillis();
        MultiThreadedMaster.totalTransportTime.addAndGet((transportEnd-transportStart));
        MultiThreadedMaster.transportTimes.add((transportEnd-transportStart));
        MultiThreadedMaster.transportPoolLatch.countDown();
        LOGGER.info("Transporter Sent Count - " + partition + " - " + MultiThreadedMaster.transporterSentCount.get());
        LOGGER.info(" Transporter " + Thread.currentThread().getName() + " exiting..");
    }
}
