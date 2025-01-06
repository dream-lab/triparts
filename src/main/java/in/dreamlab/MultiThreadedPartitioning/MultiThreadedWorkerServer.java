package in.dreamlab.MultiThreadedPartitioning;

import in.dreamlab.MultiThreadedPartitioning.serviceHandler.MultiThreadedWorkerServiceHandler;
import in.dreamlab.MultiThreadedPartitioning.thrift.MultiThreadedWorkerService;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MultiThreadedWorkerServer {
    public static MultiThreadedWorkerServiceHandler handler;
    public static MultiThreadedWorkerService.Processor eventProcessor;
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedWorkerServer.class);

    protected static int N_THREADS = 0;

    protected static ConcurrentHashMap<Long, Integer> threadIdMap = new ConcurrentHashMap<>();

    protected static AtomicInteger threadCount = new AtomicInteger(0);

    // Incremental triangle vertices set since last sync
    protected static ConcurrentHashMap<Long,Boolean> triMap;
    protected static ConcurrentHashMap.KeySetView<Long,Boolean> triSet;
    protected static ReentrantReadWriteLock triSetReadWriteLock;

    protected static TServerTransport serverTransport;
    protected static TServer server;

    public static ExecutorService createExecutorService() {
        LinkedBlockingQueue<Runnable> executorQueue = new LinkedBlockingQueue<>();
        return new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, executorQueue);
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int wID = Integer.parseInt(args[1]);
        String method = args[2];
        Integer handlerThreadCount = Integer.parseInt(args[3]);
        N_THREADS = handlerThreadCount;
        ExecutorService workerExecutorService = Executors.newFixedThreadPool(handlerThreadCount);
        triMap = new ConcurrentHashMap<Long,Boolean>(1000000, 0.75f, handlerThreadCount+1);
        triSet = triMap.keySet(Boolean.TRUE);
        triSetReadWriteLock = new ReentrantReadWriteLock();
        MultiThreadedWorker worker = new MultiThreadedWorker(port,wID,method, workerExecutorService);
        handler = new MultiThreadedWorkerServiceHandler(worker);
        eventProcessor = new MultiThreadedWorkerService.Processor(handler);


        // THREAD POOL SERVER IMPLEMENTATION
        try {
            serverTransport = new TServerSocket(port);
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(eventProcessor));
            LOGGER.info("MULTITHREADED_WORKER.START,PORT," + port + ",TIME.MS," + System.currentTimeMillis());
           server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }
}
