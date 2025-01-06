package in.dreamlab.MultiThreadedPartitioning;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.util.concurrent.AtomicDouble;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TByteIntHashMap;
import gnu.trove.map.hash.TLongByteHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TByteHashSet;
import in.dreamlab.MultiThreadedPartitioning.thrift.MessageType;
import in.dreamlab.MultiThreadedPartitioning.thrift.MultiThreadedWorkerService;
import in.dreamlab.StreamingPartitioning.WorkerInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MultiThreadedMaster {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedMaster.class);

    // Information pertaining to each worker, for thrift communication purposes
    protected static Map<Integer, WorkerInformation> workerInfo = new HashMap<>();

    // total vertices in the graph
    private static int totalVertices;

    // total edges in the graph
    protected static int totalEdges;

    // current average degree of graph
    protected static float currentAvgDegree;

    // maximum capacity of a partition = total edges/# partitions.
    protected static int maxPartitionCapacity;

    protected static Integer countEdge = 0;

    // lower and higher watermark for partition threshold while computation is running
    protected static int lowerThreshold, higherThreshold;

    // booleans to indicate whether watermark values need to be incremented
    private static boolean increaseLowerThreshold, increaseUpperThreshold;

    // array to store count of edges in all partitions
//    protected static int[] currentPartitionSizes;

//    protected static AtomicIntegerArray currentPartitionSizes;

    protected static AtomicInteger[] currentPartitionSizes;

    protected static Object[] per_partition_lock;

    // mainted for current vertex count. used to calculate average degree
    protected static BloomFilter<Long> vertexEstimator;

    // heuristic which is to be evaluated
    protected static MultiThreadedHeuristic heuristic;

    // array of bloom filters to approximately identify presence of vertices on the partitions
    protected static BloomFilter<Long>[] bloomFilterArray;

    // set of final data structures being used
    protected static TLongObjectHashMap<TByteHashSet> vertexTrianglePartitionMap;
    protected static TLongByteHashMap vertexFirstTrianglePartitionMap;
    protected static TLongObjectHashMap<TByteIntHashMap> vertexHDPartitionDegreeMap;

    protected static ArrayList<String>[] edgePartitionQueue;

    // shared producer consumer queue per partition
    protected static ConcurrentLinkedQueue<Edge>[] transportQueues;

    protected static Set<String> ogEdgeList = new HashSet<>();

    // total number of partitions
    protected static byte numPartitions;

    // interval counter
    protected static AtomicInteger intervalCounter;
    protected static AtomicInteger syncCounter;

    // sync interval
    protected static int syncInterval;

    protected static long syncEdgeCount = 0;

    protected static Boolean getWorkerStateFlag = Boolean.FALSE;

    protected static CountDownLatch readerLatch;

    protected static CountDownLatch computePoolLatch;

    protected static CountDownLatch transportPoolLatch;

    protected static AtomicLong edgeCountDownPartitioner;

    protected static AtomicLong edgeCountDownTransporter;

    protected static AtomicLong transporterSentCount;

    protected static AtomicBoolean updateRequired;

    protected static Semaphore stateUpdateLock;

    protected static Object stateUpdateObj = new Object();

    // Config
    protected static Long batchSize;
    protected static Long threshold;

    // Locks for IO synchronization
    protected static AtomicLong batchCounter;
    protected static AtomicLong readCount;
    protected static AtomicLong consumedCount;
    protected static AtomicBoolean isReading;

    // Locks for shared data structures
    protected static Object triangleMapLock = new Object();
    protected static Object firstTriangleMapLock = new Object();
    protected static Object hdMapLock = new Object();

    protected static Long total_growth_logging_time = 0L;
    protected static ArrayList<Long> per_sync_growth_logging_time = new ArrayList<>();

    protected static AtomicLong totalComputeTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalIOQueueReadTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalDecisionTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalAvgDegreeCalTime = new AtomicLong(0L);
    protected static AtomicLong compute_waitBeforePartitionSizeUpdate = new AtomicLong(0L);
    protected static AtomicLong compute_totalBloomFilterUpdateTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalPartitionSizeUpdateTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalUpdateThresholdTime= new AtomicLong(0L);
    protected static AtomicLong compute_totalQueuePutTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalLastEdgeAddTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalSyncWaitTime = new AtomicLong(0L);
    protected static AtomicLong compute_IOReadTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalBusyWaitingTime = new AtomicLong(0L);

    // decision utility function timings (split ups) - total Time (for per thread divide by 8)
    protected static AtomicLong compute_total_getLightestPartition = new AtomicLong(0L);
    protected static AtomicLong compute_total_getPartitionOfMaxDegreeFromTwoSets = new AtomicLong(0L);
    protected static AtomicLong compute_total_getLeastLoadedPartition = new AtomicLong(0L);
    protected static AtomicLong compute_total_getPartitionOfMaxDegreeVertexFromSet = new AtomicLong(0L);
    protected static AtomicLong compute_total_getMaxDegreeSumPartitions = new AtomicLong(0L);
    protected static AtomicLong compute_total_getPartitionOfIntersectionSets = new AtomicLong(0L);
    protected static AtomicLong compute_total_TwoSetIntersectionTime = new AtomicLong(0L);
    protected static AtomicLong compute_total_ThreeSetIntersectionTime = new AtomicLong(0L);
    protected static AtomicLong compute_total_setUnionTime = new AtomicLong(0L);
    protected static AtomicLong compute_total_getPartitionUnionOfSets = new AtomicLong(0L);
    protected static AtomicLong compute_total_getVertexBFSet = new AtomicLong(0L);

    // sync split ups
    protected static AtomicLong compute_totalCheckIfSyncReachedTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalWaitForComputeToFinish = new AtomicLong(0L);
    protected static AtomicLong compute_totalWaitForTransportToFinish = new AtomicLong(0L);
    protected static AtomicLong compute_totalFetchTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalSyncNotifyTime = new AtomicLong(0L);
    protected static AtomicLong compute_totalIOSyncTime = new AtomicLong(0L);

    protected static AtomicLong totalTransportTime = new AtomicLong(0L);
    protected static AtomicLong transport_totalSendTime = new AtomicLong(0L);
    protected static AtomicLong total_transport_wait_before_lock = new AtomicLong(0L);
    protected static AtomicLong total_transport_wait_time = new AtomicLong(0L);
    protected static AtomicLong total_transport_queue_read_time = new AtomicLong(0);
    protected static AtomicLong total_transport_check_and_transport = new AtomicLong(0);

    protected static AtomicLong totalReaderTime = new AtomicLong(0L);
    protected static AtomicLong reader_totalBatchReadTime = new AtomicLong(0L);
    protected static AtomicLong reader_totalNotifyingTime = new AtomicLong(0L);
    protected static AtomicLong reader_totalIOWaitTime = new AtomicLong(0L);

    // per sync
    protected static List<Long> per_sync_wait_for_compute = new ArrayList<>();
    protected static List<Long> per_sync_wait_for_transport = new ArrayList<>();
    protected static List<Long> per_sync_time = new ArrayList<>();

    protected static List<Long> transportTimes = new ArrayList<>();
    protected static List<Long> computeTimes = new ArrayList<>();
    protected static List<Long> syncQueueSizes = new ArrayList<>();

    protected static Set<Integer> sync_points_degree_change = new HashSet<>();
    protected static List<Integer> sync_points_edge_count = new ArrayList<>();
    protected static List<String> sync_points_avg_degree_change = new ArrayList<>();

    // counters for every 10% edge ingestion
    protected static Long TEN_PERCENT_OF_TOTAL_EDGES;
    protected static Long INGESTION_INTERVAL_COUNTER;

    protected static List<Integer> TMapGrowth = new ArrayList<>();
    protected static List<Integer> HDMapGrowth = new ArrayList<>();

    protected static AtomicDouble newAvgDegree = new AtomicDouble(0.00);

    protected static AtomicBoolean updateConditionTransporter = new AtomicBoolean(false);

    protected static List<Long> computeCount = new ArrayList<>();
    protected static List<Long> transportCount = new ArrayList<>();
    protected static List<Integer> addAllQueueSize = new ArrayList<>();

    protected static AtomicInteger[] hitRate = new AtomicInteger[75];

    protected static AtomicInteger waitCount;
    protected static int SYNC_THREAD_COUNT;
    protected static int SYNC_THREAD_COUNT_TRANSPORTER;
    protected static int NUM_THREADS;
    protected static int ACTIVE_THREAD_COUNT = 0;
    protected static Object syncThreadLockPartition = new Object();
    protected static AtomicInteger decisionHandlerEdgeCount = new AtomicInteger(0);
    protected static AtomicLong processedEdgeCount = new AtomicLong(0);
    protected static AtomicReferenceArray<Edge> partitionLastProcessedEdge;
    protected static HashMap<Integer, ArrayList<Long>> per_partition_sync_time;
    protected static HashMap<Integer, ArrayList<Long>> per_partition_sync_ops_time;

    // total number of threads for processing edges
    private static int MAX_T = Integer.MAX_VALUE;


    private static int TRANSPORT_QUEUE_CAPACITY = 1000;

    // connection to all Workers are stored in this map
     protected static Map<Integer, TTransport> socketInfoMap = new HashMap<>();
    protected static Map<Integer, TTransport> syncInfoMap = new HashMap<>();
    protected static Map<Integer, MultiThreadedWorkerService.Client> workerClientMap = new HashMap<>();
    protected static Map<Integer, MultiThreadedWorkerService.Client> syncClientMap = new HashMap<>();
    protected static Map<Integer, MultiThreadedWorkerService.AsyncClient> asyncWorkerClientMap = new HashMap<>();
    protected static Map<Integer, MultiThreadedWorkerService.AsyncClient> asyncSyncClientMap = new HashMap<>();


    protected static ArrayList<Long> batch_processed_edge_count = new ArrayList<>();
    protected static ArrayList<Long> batch_read_edge_count = new ArrayList<>();
    protected static ArrayList<Long> batch_transported_edge_count = new ArrayList<>();
    protected static ArrayList<Long> batch_timestamps = new ArrayList<>();
    protected static HashMap<Integer, ArrayList<Long>> batch_transported_edge_count_per_partition = new HashMap<Integer, ArrayList<Long>>();
    protected static HashMap<Integer, ArrayList<Long>> batch_processed_edge_count_per_partition = new HashMap<Integer, ArrayList<Long>>();

    protected static BufferedWriter histogramWriter;
    protected static long SNAPSHOT_EDGE_COUNT = 1000;
    protected static ArrayList<String> histogramSnapShots = new ArrayList<>();


    public static void init(byte numParts, MultiThreadedHeuristic method, int threadCount, int numV, int numE) {
        numPartitions = numParts;
        heuristic = method;
        currentPartitionSizes = new AtomicInteger[numPartitions];
        per_partition_lock = new Object[numPartitions];
        MAX_T = threadCount;
        totalVertices = numV;
        totalEdges = numE;

        maxPartitionCapacity = totalEdges/numPartitions + ((totalEdges % numPartitions == 0) ? 0 : 1);// + totalEdges/(10*numPartitions));

        bloomFilterArray = new BloomFilter[numPartitions];
        edgePartitionQueue = new ArrayList[numPartitions];

        transportQueues = new ConcurrentLinkedQueue[numPartitions];

        lowerThreshold = maxPartitionCapacity/20 + ((maxPartitionCapacity % 20 == 0) ? 0 : 1); // 5% of partition capacity
        higherThreshold = maxPartitionCapacity/5 + ((maxPartitionCapacity % 5 == 0) ? 0 : 1); // 20% of partition capacity

        for (int i = 0; i < numPartitions; ++i) {
            per_partition_lock[i] = new Object();
            // Set the expected insertions to be #V.
            BloomFilter<Long> filter = BloomFilter.create(Funnels.longFunnel(), totalVertices, 0.01);
            bloomFilterArray[i] = filter;

            ArrayList<String> perPartQueue = new ArrayList<String>(20);
            currentPartitionSizes[i] = new AtomicInteger(0);
            edgePartitionQueue[i] = perPartQueue;
            transportQueues[i] = new ConcurrentLinkedQueue<>();
        }

        if(method == MultiThreadedHeuristic.BTH || method == MultiThreadedHeuristic.BTHE || method == MultiThreadedHeuristic.BH) {
            vertexEstimator = BloomFilter.create(Funnels.longFunnel(), totalVertices, 0.01);
        }

        for (int i = 0; i < 75; i++) {
            MultiThreadedMaster.hitRate[i] = new AtomicInteger(0);
        }

        // Initiate Latches
        readerLatch = new CountDownLatch(1);
        computePoolLatch = new CountDownLatch(threadCount);
        transportPoolLatch = new CountDownLatch(numParts);

        // Initiate Synchronisation locks
        batchCounter = new AtomicLong(0);
        readCount = new AtomicLong(0);
        consumedCount = new AtomicLong(0);
        isReading = new AtomicBoolean(false);

        edgeCountDownTransporter = new AtomicLong(numE);
        transporterSentCount = new AtomicLong(0);
        transporterSentCount = new AtomicLong(0);
        updateRequired = new AtomicBoolean(false);
        stateUpdateLock = new Semaphore(1);
        vertexFirstTrianglePartitionMap = new TLongByteHashMap();
        vertexTrianglePartitionMap = new TLongObjectHashMap<>();
        vertexHDPartitionDegreeMap = new TLongObjectHashMap<>();
        SYNC_THREAD_COUNT = threadCount - 1;
        SYNC_THREAD_COUNT_TRANSPORTER = numParts;
        NUM_THREADS = threadCount;
        TEN_PERCENT_OF_TOTAL_EDGES = Integer.toUnsignedLong((totalEdges/10));
        INGESTION_INTERVAL_COUNTER = 1L;
        waitCount = new AtomicInteger(0);
        intervalCounter = new AtomicInteger(1);
        syncCounter = new AtomicInteger(0);
        partitionLastProcessedEdge = new AtomicReferenceArray<>(numPartitions);

            per_partition_sync_time = new HashMap<>(numPartitions);
            per_partition_sync_ops_time = new HashMap<>(numPartitions);
            for (byte id = 0; id < numPartitions; id++) {
                per_partition_sync_time.put(Byte.toUnsignedInt(id), new ArrayList<>());
                per_partition_sync_ops_time.put(Byte.toUnsignedInt(id), new ArrayList<>());
            }
    }

    public static void parseConfFile(String confFile) {
        BufferedReader confFileReader;
        int index, port;
        String ip;
        /* Read the configuration file and set up worker data */
        try {
            confFileReader = new BufferedReader(new FileReader(confFile));
            String line;
            String[] confFileData = new String[3];
            while ((line = confFileReader.readLine()) != null) {
                confFileData = line.split(",");
                ip = confFileData[0];
                port = Integer.parseInt(confFileData[1]);
                index = Integer.parseInt(confFileData[2]);
                WorkerInformation worker = new WorkerInformation(ip, port);
                workerInfo.put(index, worker);
                LOGGER.info(line);
            }
            LOGGER.info("TIME_MS," + System.currentTimeMillis() +",RESERVOIR.CONFIG_INFO_GENERATED");
            confFileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void closeConnectionToPartition(MultiThreadedWorkerService.Client conn) {
        try {
            conn.closeConnection(MessageType.EXIT);
        } catch (TException e) {
            e.printStackTrace();
        }
    }



    private static void setSocketInfo() {
        for (int i = 0; i < numPartitions; i++) {
            String ip = workerInfo.get(i).getIP();
            int port = workerInfo.get(i).getPort();
            TTransport transport = new TSocket(ip, port);
            TTransport transport1 = new TSocket(ip, port);
            socketInfoMap.put(i, transport);
            syncInfoMap.put(i, transport1);
            try {
                LOGGER.info("SETTING SOCKET FOR PARTITION - " + i + " ip - " + ip + " port - " + port);
                socketInfoMap.get(i).open();
                syncInfoMap.get(i).open();
                TProtocol protocol = new TBinaryProtocol(socketInfoMap.get(i));
                TProtocol protocol1 = new TBinaryProtocol(syncInfoMap.get(i));
                //Maintain a map from partition ID to WorkerService.Client. Avoids creation of objects.
                MultiThreadedWorkerService.Client workerClient = new MultiThreadedWorkerService.Client(protocol);
                MultiThreadedWorkerService.Client syncClient = new MultiThreadedWorkerService.Client(protocol1);
                workerClientMap.put(i, workerClient);
                syncClientMap.put(i, syncClient);
          } catch (TTransportException e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",RESERVOIRDATA: All Sockets set!");
    }

    public static void updateThreshold() {
        for (int i = 0; i < currentPartitionSizes.length; i++) {
            if (currentPartitionSizes[i].get() >= lowerThreshold) {
                increaseLowerThreshold = true;
            } else {
                increaseLowerThreshold = false;
                break;
            }
        }

        /* Update lower threshold by 5% capacity */
        if (increaseLowerThreshold == true) {
            if ((maxPartitionCapacity/20) < 1) {
                lowerThreshold += 1;
            } else {
                lowerThreshold += (maxPartitionCapacity/20);
            }

            if (lowerThreshold >= maxPartitionCapacity) {
                lowerThreshold = (maxPartitionCapacity *3)/2;
            }

            if ((higherThreshold - lowerThreshold) < (maxPartitionCapacity/10)) {
                if ((maxPartitionCapacity/10) < 1) {
                    higherThreshold += 1;
                } else {
                    higherThreshold += (maxPartitionCapacity/10);
                }
                if (higherThreshold >= maxPartitionCapacity) {
                    higherThreshold = (maxPartitionCapacity *3)/2;
                }
            }
        }

         for (int i = 0; i < currentPartitionSizes.length; i++) {
            if (currentPartitionSizes[i].get() >= higherThreshold) {
                increaseUpperThreshold= true;
            } else {
                increaseUpperThreshold = false;
                break;
            }
        }

        if (increaseUpperThreshold == true) {
            if ((maxPartitionCapacity/10) < 1) {
                higherThreshold += 1;
            } else {
                higherThreshold += (maxPartitionCapacity/10);
            }
            if (higherThreshold >= maxPartitionCapacity) {
                higherThreshold = (maxPartitionCapacity *3)/2;
            }
        }
    }

    /**
     * Receive map with triangle vertices
     * and high deg vertices
     * from one of the Workers
     * @param partitionID
     * @param currentAvgDeg
     * @return
     */
    private static Map<String,ByteBuffer> recvFromPartitions(int partitionID, int currentAvgDeg, Edge lastSentEdge) {
        Map<String,ByteBuffer> specialVertexMap;
        MultiThreadedWorkerService.Client workerClient =  syncClientMap.get(partitionID);

        try {
            // If a degree based sync is triggered at the very start when compute threads haven't processed their edges
            if (lastSentEdge == null) {
                specialVertexMap = workerClient.specialVertices(MessageType.SYNC, currentAvgDeg, -1, -1);
            } else {
                specialVertexMap = workerClient.specialVertices(MessageType.SYNC, currentAvgDeg, lastSentEdge.getSource(), lastSentEdge.getSink());
            }

        } catch (TException e) {
            LOGGER.error("TIME_MS," + System.currentTimeMillis() + ",ERROR,RECV_MQC_VERTICES,IP," + workerInfo.get(partitionID).getIP()
                    + ",PORT," + workerInfo.get(partitionID).getPort());
            specialVertexMap = null;
            e.printStackTrace();
        }
        return specialVertexMap;
    }


     /**
     * Get State Update from Worker and Update trianglePartitionMap and HDPartitionDegreeMap
     * @param partitionID
     * @param newAvgDegree
     */
    private static void getStateFromWorker(byte partitionID, float newAvgDegree, Edge lastSentEdge) {

        Map<String, ByteBuffer> specialVerticesReceived; // Vertices received from worker to be added to global sets
    	LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",RESERVOIR.STATE_UPDATE_RESPONSE");

        long syncTimeStart = System.currentTimeMillis();
        specialVerticesReceived = recvFromPartitions(partitionID, Math.round(newAvgDegree), lastSentEdge);
        long syncTimeEnd = System.currentTimeMillis();
        per_partition_sync_time.get(Byte.toUnsignedInt(partitionID)).add(syncTimeEnd-syncTimeStart);

        long applyTimeStart = System.currentTimeMillis();
        ByteBuffer triangleVertRecvByteStream = specialVerticesReceived.get("mqc");
        ByteBuffer hdVertRecvByteStream = specialVerticesReceived.get("star");

        /*
         * Maintaining partition IDs in a Set; Keep adding partitions if not added earlier.
         * vertexFirstTrianglePartitionMap stores first partition where v_i forms a triangle aka home partition
         * */

        //Set the marker to the start so that Stream can be traversed
        triangleVertRecvByteStream.rewind();
        hdVertRecvByteStream.rewind();

        while (triangleVertRecvByteStream.position() + 8 < triangleVertRecvByteStream.capacity()) {
            long triangleVertex = triangleVertRecvByteStream.getLong();
            // remove synchronised for iterative getState() request
            synchronized (triangleMapLock) {
                TByteHashSet triangleVertexPartitionSet = vertexTrianglePartitionMap != null ? vertexTrianglePartitionMap.get(triangleVertex) : null;

                if (triangleVertexPartitionSet == null) {
                    triangleVertexPartitionSet = new TByteHashSet(numPartitions);
                    //since this is first instance of vertex forming triangle, keep the first partition separately
                    vertexFirstTrianglePartitionMap.put(triangleVertex, partitionID);
                }
                triangleVertexPartitionSet.add(partitionID);
                vertexTrianglePartitionMap.put(triangleVertex, triangleVertexPartitionSet);
            }
        }



        switch (heuristic) {
            case BH:
            case BTH:
            case BTHE:
                while (hdVertRecvByteStream.position() < hdVertRecvByteStream.capacity()) {
                    long hdVert = hdVertRecvByteStream.getLong();
                    int vertexDegree = (int)(hdVert >>> 40);//Long degree = hdVert >>> 40;

                    long vertexID = hdVert & 1099511627775L;
                    synchronized (hdMapLock) {
                        TByteIntHashMap partitionDegMap = vertexHDPartitionDegreeMap.get(vertexID);

                        if (partitionDegMap == null) { //YS:TODO: Costly constructor.
                            partitionDegMap = new TByteIntHashMap(numPartitions);
                            vertexHDPartitionDegreeMap.put(vertexID, partitionDegMap);
                        }
                        partitionDegMap.put(partitionID, vertexDegree);
                    }
                }

                break;

            default:
                break;
        }
        long applyTimeEnd = System.currentTimeMillis();
        per_partition_sync_ops_time.get(Byte.toUnsignedInt(partitionID)).add(applyTimeEnd-applyTimeStart);

    }


    public static void getPostPartitioningStats() {
        LOGGER.info("--- PARTITIONING COMPLETE --- ");
        int numParts = edgePartitionQueue.length;
        int totalEdges = 0;

        for (ArrayList<String> partition : edgePartitionQueue) {
            totalEdges += partition.size();
        }

        LOGGER.info("TRIANGLE MAP SIZE - " +  vertexTrianglePartitionMap.size());
        LOGGER.info("TOTAL EDGES - " + totalEdges + "\n");
    }

    // a temporary verification function
    public static boolean verify() {
        Set<String> partitionSetUnion = new HashSet<>();
        for(int i = 0; i < numPartitions; i++) {
            partitionSetUnion.addAll(edgePartitionQueue[i]);
        }

        Set<String> intersectionSet = Sets.intersection(ogEdgeList, partitionSetUnion);

        LOGGER.info("SIZE OF THE INTERSECTION SET - " + intersectionSet.size());

        return intersectionSet.size() == ogEdgeList.size();
    }


    public static void getStateFromWorkers(float newAvgDegree, Edge[] lastProcessedEdges) {
        if(heuristic == MultiThreadedHeuristic.B || heuristic == MultiThreadedHeuristic.BT) {

        } else if (heuristic == MultiThreadedHeuristic.BTH || heuristic == MultiThreadedHeuristic.BTHE || heuristic == MultiThreadedHeuristic.BH) {
            vertexHDPartitionDegreeMap.clear();
        }

        // Initiate parallel fetching of states.
        ExecutorService getStateThreadPool = Executors.newFixedThreadPool(numPartitions);
        for(byte partitionId = 0; partitionId < numPartitions; partitionId++) {
            byte id = partitionId;
            getStateThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    getStateFromWorker(id , newAvgDegree, lastProcessedEdges[id]);
                }
            });
        }

        try {
            getStateThreadPool.shutdown();
            getStateThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long growth_logs_start = System.currentTimeMillis();
        TMapGrowth.add(vertexTrianglePartitionMap.size());
        HDMapGrowth.add(vertexHDPartitionDegreeMap.size());
        long growth_logs_end = System.currentTimeMillis();
        total_growth_logging_time += (growth_logs_end-growth_logs_start);
        per_sync_growth_logging_time.add((growth_logs_end-growth_logs_start));
    }

    public static void writeAllEdges() {
        ExecutorService writerThreadPool = Executors.newFixedThreadPool(numPartitions);
        for(int partitionId = 0; partitionId < numPartitions; partitionId++) {
            LOGGER.info("INSTRUCTING PARTITION - " + partitionId);
            int id = partitionId;
            writerThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    MultiThreadedWorkerService.Client workerClient = workerClientMap.get(id);
                    try {
                        workerClient.writeEdgeToFile();
                    } catch (TException e) {
                        LOGGER.error("TIME_MS," + System.currentTimeMillis() + ",ERROR,SEND_EDGE,IP," + workerInfo.get(id).getIP()
                                + ",PORT," + workerInfo.get(id).getPort());
                        e.printStackTrace();
                    }
                }
            });
        }

        try {
            writerThreadPool.shutdown();
            writerThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
           e.printStackTrace();
        }

    }

    public static void parallelWriteAll() {
        for (int i = 0; i < numPartitions; i++) {
            int partitionId = i;
            Thread t = new Thread() {
                @Override
                public void run() {
                    LOGGER.info("INSTRUCTING PARTITION - " + partitionId);
                    MultiThreadedWorkerService.Client workerClient =  workerClientMap.get(partitionId);

                    try {
                        workerClient.writeEdgeToFile();
                    } catch (TException e) {
                        LOGGER.error("TIME_MS," + System.currentTimeMillis() + ",ERROR,SEND_EDGE,IP," + workerInfo.get(partitionId).getIP()
                                + ",PORT," + workerInfo.get(partitionId).getPort());
                        e.printStackTrace();
                    }
                }
            };

            t.start();
        }
    }

    public static void getPartitionSizes() {
        for(int i = 0; i < numPartitions; i++) {
            long elementCount = bloomFilterArray[i].approximateElementCount();
            long partitionSize = currentPartitionSizes[i].get();
            LOGGER.info("Partition " + i + " - " + "BF SIZE = " + elementCount + " SIZE = " + partitionSize);
        }
    }

    /**
     * Call for BF partitions. Return a list
     * @param vertex
     * @return
     */
    private static TIntList getVertexBFList(long vertex) {

        TIntList vertexReplicaPartitions = new TIntArrayList(numPartitions);

        for (int i = 0; i < numPartitions; i++) {
            if (bloomFilterArray[i].mightContain(vertex)) {
                vertexReplicaPartitions.add(i);
            }
        }
        return vertexReplicaPartitions;
    }

    public static double getVertexReplicationFactor() {
        double factor = 0.000;
        for (long v = 1; v < totalVertices; v++) {
            factor +=  getVertexBFList(v).size();
        }
        return (factor/totalVertices);
    }

    public static void printHistogramStats() {
        LOGGER.info("-------- Printing decision count for historgram stats -------");
        for(int i = 0; i < 75; i++) {
            LOGGER.info(i + " : " + hitRate[i].get());
        }
    }

    public static void main(String[] args) throws InterruptedException {

        LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",MULTITHREADED_MASTER.SERVICES_BEGIN");

        String edgeListFile = null; //Input file for edge list
        String method = null; // Heuristic that is to be used
        int threadCount = -1;
        int numVertices = -1;
        int numEdges = -1;
        byte nPart = 0;
        boolean moreLines = true;

        // -- parse graph parameters -- //
        BufferedReader confFileReader;
        try {
            confFileReader = new BufferedReader(new FileReader(args[0]));
            String line;
            String[] confFileData;
            while ((line = confFileReader.readLine()) != null) {
                confFileData = line.split(":");
                String parameterName = confFileData[0];
                if (parameterName.compareTo("edge-list") == 0) {
                    edgeListFile = confFileData[1]; //Input file for edge list
                } else if (parameterName.compareTo("num-parts") == 0) {
                    numPartitions = Byte.parseByte(confFileData[1]); //Number of partitions = number of bloom filters
                } else if (parameterName.compareTo("method") == 0) {
                    method = confFileData[1];
                }
                else if (parameterName.compareTo("threadCount") == 0) {
                    threadCount = Integer.parseInt(confFileData[1]);
                }
                else if (parameterName.compareTo("num-vertices") == 0) {
                    numVertices = Integer.parseInt(confFileData[1]);
                } else if (parameterName.compareTo("num-edges") == 0) {
                    numEdges = Integer.parseInt(confFileData[1]);
                } else if (parameterName.compareTo("batch-size") == 0) {
                    batchSize = Long.parseLong(confFileData[1]);
                } else if (parameterName.compareTo("threshold") == 0) {
                    threshold = Long.parseLong(confFileData[1]);
                } else if (parameterName.compareTo("sync-interval") == 0) {
                    syncInterval = Integer.parseInt(confFileData[1]);
                }
            }
            LOGGER.info("TIME_MS," + System.currentTimeMillis() +",RESERVOIR.CONFIG_INFO_GENERATED");
            confFileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        currentAvgDegree = 0.0f;
        parseConfFile(args[1]);
        setSocketInfo();

        init(numPartitions, MultiThreadedHeuristic.valueOf(method), threadCount, numVertices, numEdges);

        // initialising executors
        ExecutorService fileReadExecutor = Executors.newSingleThreadExecutor();
        ExecutorService partitionThreadExecutor = Executors.newFixedThreadPool(threadCount);
        ExecutorService transportThreadExecutor = Executors.newFixedThreadPool(numPartitions);

        ReaderThread reader = new ReaderThread(edgeListFile, batchSize, IOSharedResources.IOQueue);

        long startTime = System.currentTimeMillis();
        LOGGER.info("TIME_MS," + startTime + ",MULTI_MASTER_DRIVER.START_EDGE_READ, " + method);

        LOGGER.info("Starting file reader");
        fileReadExecutor.execute(reader);

        for(int pThread = 0; pThread < threadCount; pThread++) {
            PartitionerThread partitionerThread = new PartitionerThread(IOSharedResources.IOQueue, transportQueues);
            partitionThreadExecutor.execute(partitionerThread);
        }

        LOGGER.info(" Starting transport threads ");
        for(int partitionId = 0; partitionId < numPartitions; partitionId++) {
            MultiThreadedEdgeTransporter transportThread = new MultiThreadedEdgeTransporter(partitionId, transportQueues[partitionId]);
            transportThreadExecutor.execute(transportThread);
        }


        try {
            readerLatch.await();
            computePoolLatch.await();
            transportPoolLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("Reader Latch Count - " + readerLatch.getCount());
        LOGGER.info("Compute Latch Count - " + computePoolLatch.getCount());
        LOGGER.info("Transport Latch Count - " + transportPoolLatch.getCount());

        fileReadExecutor.shutdown();
        partitionThreadExecutor.shutdown();
        transportThreadExecutor.shutdown();

        // await termination here.

        LOGGER.info("---------- TOTAL READER THREAD TIMINGS --------");
        LOGGER.info("TOTAL READER TIME - " + MultiThreadedMaster.totalReaderTime.get());
        LOGGER.info("TOTAL BATCH READ TIME - " + MultiThreadedMaster.reader_totalBatchReadTime.get());
        LOGGER.info("TOTAL NOTIFYING TIME - " + MultiThreadedMaster.reader_totalNotifyingTime.get());
        LOGGER.info("TOTAL IO WAITING TIME - " + MultiThreadedMaster.reader_totalIOWaitTime.get());

        LOGGER.info("---------- TOTAL COMPUTE THREADS TIMINGS --------");
        LOGGER.info("TOTAL PARTITIONER TIME - " + MultiThreadedMaster.totalComputeTime.get());
        LOGGER.info("COMPUTE TIMES - " + computeTimes.toString());
        LOGGER.info("TOTAL IO QUEUE READ TIME - sum(t2-t1) " + MultiThreadedMaster.compute_totalIOQueueReadTime.get());
        LOGGER.info("COMPUTE TOTAL DECISION TIME sum(t3-t2) - " + MultiThreadedMaster.compute_totalDecisionTime.get());
        LOGGER.info("COMPUTE TOTAL AVERAGE DEGREE CALCULATION TIME - " + MultiThreadedMaster.compute_totalAvgDegreeCalTime.get());
        LOGGER.info("COMPUTE TOTAL UPDATE BLOOM FILTER TIME - " + MultiThreadedMaster.compute_totalBloomFilterUpdateTime.get());
        LOGGER.info("COMPUTE TOTAL WAIT BEFORE PARTITION SIZE UPDATE - " + MultiThreadedMaster.compute_waitBeforePartitionSizeUpdate.get());
        LOGGER.info("COMPUTE TOTAL PARTITION SIZE UPDATE TIME - " + MultiThreadedMaster.compute_totalPartitionSizeUpdateTime.get());
        LOGGER.info("COMPUTE TOTAL UPDATE THRESHOLD TIME - " + MultiThreadedMaster.compute_totalUpdateThresholdTime.get());
        LOGGER.info("COMPUTE TOTAL QUEUE PUT TIME  - " + MultiThreadedMaster.compute_totalQueuePutTime.get());
        LOGGER.info("COMPUTE TOTAL SYNC TIME - " + MultiThreadedMaster.compute_totalSyncWaitTime.get());
        LOGGER.info("COMPUTE TOTAL BUSY WAITING TIME I/O if edge null - " + MultiThreadedMaster.compute_totalBusyWaitingTime.get());
        LOGGER.info("COMPUTE TOTAL IO READ TIME - " + MultiThreadedMaster.compute_IOReadTime.get());


        LOGGER.info("--------- TRANSPORT THREADS TIMINGS --------");
        LOGGER.info("TOTAL TRANSPORT TIME = " + MultiThreadedMaster.totalTransportTime.get());
        LOGGER.info("TRANSPORT TIMES - " + transportTimes.toString());
        LOGGER.info("TOTAL TRANSPORTER WAIT TIME BEFORE SYNC LOCK - " + MultiThreadedMaster.total_transport_wait_before_lock.get());
        LOGGER.info("TOTAL TRANSPORTER WAIT TIME AT SYNC - " + MultiThreadedMaster.total_transport_wait_time.get());
        LOGGER.info("TOTAL TRANSPORTER TIME READ QUEUE  sum(t5-t4) - " + MultiThreadedMaster.total_transport_queue_read_time.get());
        LOGGER.info("TOTAL TRANSPORTER RPC CALL sum(t6-t5) - " + MultiThreadedMaster.transport_totalSendTime.get());

        LOGGER.info("-------- TOTAL TIMINGS FOR DECISION UTILITY SPLITS ( for per thread x/num_threads ) --------- ");
        LOGGER.info("TOTAL getLightestPartition - " + compute_total_getLightestPartition.get());
        LOGGER.info("TOTAL getPartitionOfMaxDegreeFromTwoSets - " + compute_total_getPartitionOfMaxDegreeFromTwoSets.get());
        LOGGER.info("TOTAL getLeastLoadedPartition - " + compute_total_getLeastLoadedPartition.get());
        LOGGER.info("TOTAL getPartitionOfMaxDegreeVertexFromSet - " + compute_total_getPartitionOfMaxDegreeVertexFromSet.get());
        LOGGER.info("TOTAL getMaxDegreeSumPartitions - " + compute_total_getMaxDegreeSumPartitions.get());
        LOGGER.info("TOTAL getPartitionOfIntersectionSets - " + compute_total_getPartitionOfIntersectionSets.get());
        LOGGER.info("TOTAL TwoSetIntersectionTime - " + compute_total_TwoSetIntersectionTime.get());
        LOGGER.info("TOTAL ThreeSetIntersectionTime - " + compute_total_ThreeSetIntersectionTime.get());
        LOGGER.info("TOTAL setUnionTime - " + compute_total_setUnionTime.get());
        LOGGER.info("TOTAL getPartitionUnionOfSets - " + compute_total_getPartitionUnionOfSets.get());
        LOGGER.info("TOTAL getVertexBFSet - " + compute_total_getVertexBFSet.get());


        LOGGER.info("---------- PER THREAD COMPUTE THREADS TIMINGS --------");
        LOGGER.info("TOTAL PARTITIONER TIME (per thread) - " + (MultiThreadedMaster.totalComputeTime.get()/threadCount));
        LOGGER.info("TOTAL IO QUEUE READ TIME - sum(t2-t1)/N (per thread) " + (MultiThreadedMaster.compute_totalIOQueueReadTime.get()/threadCount));
        LOGGER.info("COMPUTE TOTAL DECISION TIME sum(t3-t2)/N (per thread) - " + MultiThreadedMaster.compute_totalDecisionTime.get()/threadCount);
        LOGGER.info("COMPUTE TOTAL AVERAGE DEGREE CALCULATION TIME - " + MultiThreadedMaster.compute_totalAvgDegreeCalTime.get()/threadCount);
        LOGGER.info("COMPUTE TOTAL UPDATE BLOOM FILTER TIME - " + MultiThreadedMaster.compute_totalBloomFilterUpdateTime.get()/threadCount);
        LOGGER.info("COMPUTE TOTAL PARTITION SIZE UPDATE TIME - " + MultiThreadedMaster.compute_totalPartitionSizeUpdateTime.get()/threadCount);
        LOGGER.info("COMPUTE TOTAL UPDATE THRESHOLD TIME - " + MultiThreadedMaster.compute_totalUpdateThresholdTime.get()/threadCount);
        LOGGER.info("COMPUTE TOTAL QUEUE PUT TIME  - " + (MultiThreadedMaster.compute_totalQueuePutTime.get()/threadCount));
        LOGGER.info("COMPUTE TOTAL SYNC TIME - (per thread)" + (MultiThreadedMaster.compute_totalSyncWaitTime.get()/threadCount));
        LOGGER.info("COMPUTE IO SYNC TIME - (per thread) " + (MultiThreadedMaster.compute_totalIOSyncTime.get()/threadCount));

        LOGGER.info("---------- SYNC TIME SPLIT ----------- ");
        LOGGER.info("TOTAL CHECK IF SYNC REACHED TIME - " + compute_totalCheckIfSyncReachedTime.get()/threadCount);
        LOGGER.info("TOTAL WAIT FOR COMPUTE TO FINISH - " + compute_totalWaitForComputeToFinish.get());
        LOGGER.info("TOTAL WAIT FOR TRANSPORT TO FINISH - " + compute_totalWaitForTransportToFinish.get());
        LOGGER.info("TOTAL FETCH TIME - " + compute_totalFetchTime.get());
        LOGGER.info("TOTAL NOTIFYING TIME  - " +  compute_totalSyncNotifyTime.get());
        LOGGER.info("-------- IO SYNC TIME ----------------");
        LOGGER.info("COMPUTE IO SYNC TIME - " + MultiThreadedMaster.compute_totalIOSyncTime.get());



        LOGGER.info("--------- PER THREAD TRANSPORT THREADS TIMINGS --------");
        LOGGER.info("TOTAL TRANSPORT TIME = (per thread)" + (MultiThreadedMaster.totalTransportTime.get()/numPartitions));
        LOGGER.info("TOTAL TRANSPORTER CHECK AND TRANSPORT - " + (MultiThreadedMaster.total_transport_check_and_transport.get()/numPartitions));
        LOGGER.info("TOTAL TRANSPORTER TIME READ QUEUE  sum(t5-t4)/M (per thread)  - " + (MultiThreadedMaster.total_transport_queue_read_time.get()/numPartitions));
        LOGGER.info("TOTAL TRANSPORTER RPC CALL sum(t6-t5)/M (per thread) - " + (MultiThreadedMaster.transport_totalSendTime.get()/numPartitions));

        LOGGER.info("--------- SYNC CONDITIONS --------");
        LOGGER.info("Compute - " + computeCount.toString());
        LOGGER.info("Transport - " + transportCount.toString());
        LOGGER.info("Queue Sizes - " + addAllQueueSize.toString());
        LOGGER.info("SYNC POINTS (EDGE % CONDITION MET) - " + sync_points_edge_count.toString());
        LOGGER.info("SYNC POINTS (DEGREE CHANGE MET) -  " + sync_points_degree_change.toString());
        LOGGER.info("SYNC POINTS AVG DEGREE COMPARISON - " + sync_points_avg_degree_change.toString());

        LOGGER.info("----------- TMAP Growth at every per sync edges ingested ----------- ");
        LOGGER.info("TMap - " + MultiThreadedMaster.TMapGrowth.toString());
        LOGGER.info("----------- HD-MAP Growth at every per sync edges ingested ----------- ");
        LOGGER.info("HDMap - " + MultiThreadedMaster.HDMapGrowth.toString());

        LOGGER.info("------------ PER SYNC TIMINGS WITH WAITING  (master) ----------- ");
        LOGGER.info("Wait for compute - " + per_sync_wait_for_compute.toString());
        LOGGER.info("Wait for transport - " + per_sync_wait_for_transport.toString());
        LOGGER.info("Wait for sync - " + per_sync_time.toString());

        LOGGER.info("------------ PER PARTITION SYNC RESPONSE TIME (master) --------- ");
        for(int id = 0; id < numPartitions; id++) {
            LOGGER.info("Partition " + id + " - " + per_partition_sync_time.get(id).toString());
        }
        LOGGER.info("------------- PER PARTITION SYNC APPLICATION TIME (master) -------");
        for(int id = 0; id < numPartitions; id++) {
            LOGGER.info("Partition " + id + " - " + per_partition_sync_ops_time.get(id).toString());
        }


        LOGGER.info("------ Count of edges entering the decision handler - " + decisionHandlerEdgeCount.get());


        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long free = rt.freeMemory();
        long used = total - free;
        LOGGER.info("TOTAL MEMORY USAGE MASTER_DRIVER.Total: " + total + ", Used: " + used + ", Free: " + free);

        LOGGER.info("----- MASTER INSTRUCTING WORKERS TO WRITE ----");
        long writeStart = System.currentTimeMillis();
        writeAllEdges();
        long writeEnd = System.currentTimeMillis();
        long total_write_time = writeEnd-writeStart;
        LOGGER.info("WRITE TIME_MS - " + (writeEnd-writeStart));
//        parallelWriteAll();

        // EXECUTION TIME -
        long total_processing_time = System.currentTimeMillis() - startTime;
        LOGGER.info("TOTAL PROCESSING TIME TAKEN - TIME_MS," + (System.currentTimeMillis()-startTime));

        LOGGER.info("------ QUALITY METRICS ----- ");
        LOGGER.info("Vertex Replication Factor - " +  getVertexReplicationFactor());

        LOGGER.info("POST PARITION VERIFICATION - " + verify());
        // post partitioning logs
        getPostPartitioningStats();
        getPartitionSizes();
        printHistogramStats();

        // file to add histogram-snapshots
        try {
            String graphName = "";
            if (edgeListFile != null) {
                String[] graphFileSplit = edgeListFile.split("/");
                graphName = graphFileSplit[5];
            }

            String histogram_file = "histogram-" + graphName + "-" + numPartitions + "p-" + method + "-" + threadCount + "t.log";
            histogramWriter = new BufferedWriter(new FileWriter(histogram_file));

            for(String snapshot : histogramSnapShots) {
                try {
                    LOGGER.info(snapshot);
                    histogramWriter.write(snapshot);
                    histogramWriter.newLine();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            histogramWriter.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }



       LOGGER.info("TOTAL GROWTH LOGGING TIME - (TIME_MS) - " + total_growth_logging_time);
        LOGGER.info("PER SYNC GROWTH LOGGING TIME - (TIME_MS) - " + per_sync_growth_logging_time.toString());
        LOGGER.info("EFFECTIVE COMPUTE TIME - (TIME_MS) - " + (total_processing_time - total_write_time - ((MultiThreadedMaster.reader_totalBatchReadTime.get()/threadCount))));
        LOGGER.info("EFFECTIVE COMPUTE TIME - (TIME_MS) (remove log time) - " + (total_processing_time - total_write_time - ((MultiThreadedMaster.reader_totalBatchReadTime.get()/threadCount)) - total_growth_logging_time));
        LOGGER.info("--- CLOSING CONNECTIONS ---");
        for(int partitionId = 0; partitionId < numPartitions; partitionId++) {
            try {
                workerClientMap.get(partitionId).closeConnection(MessageType.EXIT);
           } catch (TException e) {
                e.printStackTrace();
            }
        }
        System.exit(0);
    }
}
