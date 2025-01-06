package in.dreamlab.MultiThreadedPartitioning;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import in.dreamlab.MultiThreadedPartitioning.thrift.MessageType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author siddharth
 * Code for a Worker when the Master is Multithreaded.
 * Get an array of edges, process them, identify newly created triangles, high degree vertices and print edges to file.
 */

public class MultiThreadedWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedWorker.class);

    protected static int CAPACITY = 40000000;

    protected static TLongObjectHashMap<TLongHashSet> partitionAdjacencyList = new TLongObjectHashMap<TLongHashSet>(CAPACITY);

    protected static List<Long> syncEdgeCountLog = new ArrayList<>();

    protected static List<Long> syncTriangleCountLog = new ArrayList<>();

    protected static List<Integer> syncVertexCountLog = new ArrayList<>();

    protected static List<Integer> syncHDSetSize = new ArrayList<>();

    protected static List<Integer> syncTriangleSetSize = new ArrayList<>();

    //All vertices that become part of some triangle in this partition- grow incrementally
    // Global set of all triangle vertices seen since the start, across all syncs
    protected static TLongHashSet triangleVertexSet = new TLongHashSet();

    //All vertices that become high degree in this partition since last sync with Master
    protected static TLongHashSet highDegVerticesSet = new TLongHashSet();

    //All vertices that have become part of some triangle since last sync and need to be sent to Master
    protected static TLongHashSet triangleVertToSend = new TLongHashSet();

    protected static Object partitionAdjacencyListLock = new Object();

    protected static Object syncStateLock = new Object();

    //Count total triangles identified (increases Monotonically)
    protected static AtomicLong numTrianglesIdentified = new AtomicLong(0L);

    protected static long syncEdgeBoundarySource = -1, syncEdgeBoundarySink = -1;
    protected static AtomicBoolean syncEdgeBoundaryPending = new AtomicBoolean(false);


    //LOGGING METRICS -------
    protected static Long startTime = 0L;

    protected static AtomicLong taskCreationTime = new AtomicLong(0L);

    protected static AtomicLong taskExecutionTime = new AtomicLong(0L);

    protected static AtomicLong totalTriangleCountCriticalSectionTime = new AtomicLong(0L);

    protected static AtomicLong totalTriangleCountNonCriticalSectionTime = new AtomicLong(0L);

    // ------- CRITICAL SECTION SPLIT TIME ----------//
    protected static AtomicLong totalAdjacencyListGetTime = new AtomicLong(0L);

    protected static AtomicLong totalAdjacencyListPutTime = new AtomicLong(0L);

    protected static AtomicLong totalHashSetAddTime = new AtomicLong(0L);

    protected static AtomicLong totalAddAllTime = new AtomicLong(0L);

    protected static AtomicLong totalWaitTimeBeforeCriticalSection = new AtomicLong(0L);

    // --------- NON CRITICAL SECTION SPLIT TIME --------//

    protected static AtomicLong totalRetainAllTime = new AtomicLong(0L);

    protected static AtomicLong totalRemainingTime = new AtomicLong(0L);

    // --------------------------------------------------//

    protected static AtomicLong totalTriangleCountTime = new AtomicLong(0L);

    protected static AtomicLong totalWaitBeforeUpdateTriangles = new AtomicLong(0L);

    protected static AtomicLong totalUpdateTrianglesAddTime = new AtomicLong(0L);

    protected static AtomicLong totalSyncTime = new AtomicLong(0L);

    protected static List<Long> perSyncTime = new ArrayList<>();

    protected static AtomicLong totalAwaitTerminationTime = new AtomicLong(0L);

    protected static AtomicLong totalWriteTime = new AtomicLong(0L);

    // ----------------------------------------------------- //

    // --------- Sync State Flag ---------- //
    protected static AtomicBoolean syncResponseSent = new AtomicBoolean(false);

    protected static AtomicLong syncSentTimestamp = new AtomicLong(0L);

    protected static AtomicLong newEdgeArrivedTimestamp = new AtomicLong(0L);

    protected static List<Long> time_between_sent_response_and_new_edge_arrival = new ArrayList<>();

    protected static List<Integer> vertexCountPerBatch = new ArrayList<>();

    // --------------- average neighbours per vertex -------------- //
    protected static AtomicLong totalNeighbourCount = new AtomicLong(0L);

    protected static AtomicLong totalNeighbourProduct = new AtomicLong(0L);

    protected static AtomicLong setCounter = new AtomicLong(0L);

    protected static AtomicLong edgeSetCounter = new AtomicLong(0L);

    protected static List<Long> averageNeighbourCount = new ArrayList<>();

    // ----------- per thread lock based on id ---------- //
    protected static HashMap<String, Long> perthreadWaitTimeBeforeCriticalSection = new HashMap<>();

    // ------------ retain all operation timings -------- //
    protected static AtomicLong totalSumOfNeighbours = new AtomicLong(0);

    protected static AtomicLong totalComparisonCount = new AtomicLong(0);

    protected static AtomicLong retainAllCount = new AtomicLong();

    protected static AtomicInteger totalIntersectionSetSize = new AtomicInteger(0);

    protected static Long totalWorkerTime = 0L;

    // sync split logs
    protected static ArrayList<Long> per_sync_get_from_partitions = new ArrayList<>();
    protected static ArrayList<Long> per_sync_wait_time_to_acquire_lock = new ArrayList<>();
    protected static ArrayList<Long> per_sync_boundary_wait = new ArrayList<>();
    protected static ArrayList<Long> per_sync_triangle_op = new ArrayList<>();
    protected static ArrayList<Long> per_sync_hd_op = new ArrayList<>();
    protected static ArrayList<Long> per_sync_avg_neighbours_per_vertex = new ArrayList<>();
    protected static ArrayList<Long> per_sync_avg_comparisons_per_edge = new ArrayList<>();
    protected static ArrayList<Long> per_sync_active_thread_count = new ArrayList<>();

    // queue logs
    protected static ArrayList<Integer> batch_total_received_edges = new ArrayList<>();
    protected static ArrayList<Long> batch_total_processed_edges = new ArrayList<>();
    protected static ArrayList<Long> batch_timestamps = new ArrayList<>();

    // Store worker ID.
    int workerID;

    MultiThreadedHeuristic heuristic;

    protected static Semaphore updateTriangleVertices = new Semaphore(1);
    protected static Semaphore syncLock = new Semaphore(1);
    protected static AtomicBoolean syncBoundaryNotification = new AtomicBoolean(false);


    /* *
     * Maintain count of red vertices, green edges for logging purpose
     * Red vertices = received after edge replication[the one which doesn't have triangle on this partition is red]
     * Green edges = received for all other cases
     * A red vertex can be removed from its set if it already has another neighbour here or gets another neighbour here
     * */
    int red1VertexCount = 0;
    int red2VertexCount = 0;

    int greenEdgeCount = 0;

    // Maintain count of edges received
    AtomicInteger totalEdgeCount = new AtomicInteger(0);
    int counter = 10;

    protected static AtomicLong totalProcessedEdgeCount = new AtomicLong(0);

    TLongHashSet red1Vertices = new TLongHashSet();
    TLongHashSet red2Vertices = new TLongHashSet();

    /* Write the adjacency list to file */
    FileWriter fWriter = null;
    ExecutorService executor;


    public MultiThreadedWorker(int port, int wID, String t, ExecutorService executor) {
        try {
            heuristic = MultiThreadedHeuristic.valueOf(t);
            workerID = wID;
            this.executor = executor;
            fWriter = new FileWriter(new File("WorkerData_" + t + "_" + workerID + ".txt"), true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutDownServer(MessageType messageType) throws TException {
        if (messageType.getValue() == MessageType.EXIT.getValue()) {
            MultiThreadedWorkerServer.server.stop();
            MultiThreadedWorkerServer.serverTransport.close();
        } else {
            throw new TException("Invalid EXIT Message");
        }
    }

    /**
     * Write all edges to file after computation completes
     * @throws IOException
     */
    public void writeEdges() throws IOException {

        LOGGER.info("TOTAL PROCESSED EDGE COUNT - " + totalProcessedEdgeCount.get() + ", TOTAL EDGE COUNT RECIEVED - " + totalEdgeCount.get());

        long awaitTerminationStart = System.currentTimeMillis();
        try {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long awaitTerminationEnd = System.currentTimeMillis();
        totalAwaitTerminationTime.addAndGet((awaitTerminationEnd-awaitTerminationStart));

        LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",FINAL,WORKER.TOTAL_TRIANGLES," +
                numTrianglesIdentified + " ,WORKER.TOTAL_EDGES," + totalEdgeCount.get() + ",WORKER.TOTAL_VERTICES," + partitionAdjacencyList.size());

        LOGGER.info("TOTAL PROCESSED EDGE COUNT - " + totalProcessedEdgeCount.get() + ", TOTAL EDGE COUNT RECIEVED - " + totalEdgeCount.get());

        long writeTimeStart = System.currentTimeMillis();
        try {
            for (long src: partitionAdjacencyList.keys()) {
                fWriter.write(workerID + "," + src + ",");

                TLongHashSet neighboursList = partitionAdjacencyList.get(src);
                for (TLongIterator iterator = neighboursList.iterator(); iterator.hasNext();) {
                    long neighbourVertex = iterator.next();
                    fWriter.write("," + neighbourVertex);
                    totalEdgeCount.decrementAndGet();
                }
                fWriter.write("\n");
                fWriter.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fWriter.close();
            LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",WORKER.EDGES_WRITTEN");
        }
        long writeTimeEnd = System.currentTimeMillis();
        totalWriteTime.addAndGet((writeTimeEnd-writeTimeStart));
        totalWorkerTime = System.currentTimeMillis() - startTime;

        LOGGER.info("TOTAL EDGE COUNT RECIEVED - (post writing) - " + totalEdgeCount.get());


        LOGGER.info("---------------- PER THREAD WAITING TIME BEFORE CRITICAL SECTION ------------- ");
        long cummWaitingTime = 0L;
        for (String key: perthreadWaitTimeBeforeCriticalSection.keySet()) {
            Long time = perthreadWaitTimeBeforeCriticalSection.get(key);
            LOGGER.info("Key -> " + key + " Time -> " + time);
            cummWaitingTime += time;
        }
        LOGGER.info("--------------- WORKER PERFORMANCE LOGS ---------------");
        LOGGER.info("TOTAL WORKER TIME (ms) - " + totalWorkerTime);
        LOGGER.info("TOTAL TASK CREATION TIME (ms) - " + taskCreationTime.get());
        LOGGER.info("TOTAL TASK EXECUTION TIME (ms) - " + taskExecutionTime.get());
        LOGGER.info("TOTAL TRIANGLE COUNTING TIME (ms) - " + totalTriangleCountTime.get());
        // ------- new logs --------- // for non critical section timings (split chunks)
        LOGGER.info("TOTAL WAIT TIME BEFORE CRITICAL SECTION TIME (ms) - "  + totalWaitTimeBeforeCriticalSection.get());
        LOGGER.info("PER THREAD WAIT TIME BEFORE CRITICAL SECTION TIME (ms) - "  + totalWaitTimeBeforeCriticalSection.get()/MultiThreadedWorkerServer.N_THREADS);
        LOGGER.info("TOTAL WAIT TIME BEFORE CRITICAL SECTION TIME (ms) (from per thread) - " + cummWaitingTime);
        LOGGER.info("AVERAGE WAIT TIME BEFORE CRITICAL SECTION TIME (ms) (from per thread) - " + (cummWaitingTime/MultiThreadedWorkerServer.N_THREADS));
        LOGGER.info("-------- CRITICAL SECTION ---------");
        LOGGER.info("TOTAL TRIANGLE COUNT CRITICAL SECTION TIME (ms) - " + totalTriangleCountCriticalSectionTime.get() + " PER THREAD - " + totalTriangleCountCriticalSectionTime.get()/MultiThreadedWorkerServer.N_THREADS);
        LOGGER.info("TOTAL ADJACENCY LIST GET TIME (ms) - " + totalAdjacencyListGetTime.get());
        LOGGER.info("TOTAL ADJACENCY LIST PUT TIME (ms) - " + totalAdjacencyListPutTime.get());
        LOGGER.info("TOTAL HASH SET ADD TIME (ms)  - " + totalHashSetAddTime.get());
        LOGGER.info("TOTAL ADD ALL TIME (ms) - " + totalAddAllTime.get());
        LOGGER.info("----------- NON CRITICAL SECTION ------------");
        LOGGER.info("TOTAL TRIANGLE COUNT NON CRITICAL SECTION TIME (ms) - " + totalTriangleCountNonCriticalSectionTime.get());
        LOGGER.info("TOTAL RETAIN ALL TIME (ms) - " + totalRetainAllTime.get()/MultiThreadedWorkerServer.N_THREADS);
        LOGGER.info("TOTAL REMAINING TIME (ms) - " + totalRemainingTime.get()/MultiThreadedWorkerServer.N_THREADS);
        LOGGER.info("------------- UPDATE TRIANGLES -------------");
        LOGGER.info("TOTAL WAIT BEFORE UPDATE TRIANGLES TIME (ms) - " + totalWaitBeforeUpdateTriangles.get()/MultiThreadedWorkerServer.N_THREADS);
        LOGGER.info("TOTAL UPDATE TRIANGLES ADD ALL TIME (ms) - " + totalUpdateTrianglesAddTime.get());
        LOGGER.info("---------- NON CRITICAL SECTION FUNCTION METRICS (Intersect operation ) --------------");
        LOGGER.info("Total Product of sizes of src,sink (comparisons for intersection operation) - " + totalComparisonCount.get() + " Average sum of size - " + (totalComparisonCount.get()/retainAllCount.get()));
        LOGGER.info("-------------- TERMINATION --------------");
        LOGGER.info("TOTAL AWAIT TERMINATION TIME (ms) - " + totalAwaitTerminationTime.get());
        LOGGER.info("TOTAL WRITE TIME (ms) - " + totalWriteTime.get());
        LOGGER.info("TOTAL SYNC TIME (ms) - " + totalSyncTime.get());
        LOGGER.info("PER SYNC TIME [(ms)] - " + perSyncTime.toString());

        LOGGER.info("---------- PER SYNC TIME BETWEEN SENT RESPONSE AND NEW EDGE ARRIVAL ---------");
        LOGGER.info("Per sync time difference - " + time_between_sent_response_and_new_edge_arrival.toString());
        LOGGER.info("Per sync wait time to acquire lock - " + per_sync_wait_time_to_acquire_lock.toString());
        LOGGER.info("Per sync get from partitions - " + per_sync_get_from_partitions.toString());
        LOGGER.info("Per sync wait for boundary edge - " + per_sync_boundary_wait.toString());
        LOGGER.info("Per sync triangle ops - " + per_sync_triangle_op.toString());
        LOGGER.info("Per sync hd ops - " + per_sync_hd_op.toString());

        LOGGER.info("---------- CUMM. SYNC TIMES ---------");
        LOGGER.info("Per sync time difference - " + time_between_sent_response_and_new_edge_arrival.stream().mapToLong(a -> a).sum());
        LOGGER.info("Per sync wait time to acquire lock - " + per_sync_wait_time_to_acquire_lock.stream().mapToLong(a -> a).sum());
        LOGGER.info("Per sync get from partitions - " + per_sync_get_from_partitions.stream().mapToLong(a -> a).sum());
        LOGGER.info("Per sync wait for boundary edge - " + per_sync_boundary_wait.stream().mapToLong(a -> a).sum());
        LOGGER.info("Per sync triangle ops - " + per_sync_triangle_op.stream().mapToLong(a -> a).sum());
        LOGGER.info("Per sync hd ops - " + per_sync_hd_op.stream().mapToLong(a -> a).sum());

        LOGGER.info("------- PER SYNC GROWTH OF AVERAGE NEIGHBOURS PER VERTEX -------- ");
        LOGGER.info("Avg Neighbours per vertex - " + per_sync_avg_neighbours_per_vertex.toString());
        LOGGER.info("Avg Comparisons per edge - " + per_sync_avg_comparisons_per_edge.toString());

        LOGGER.info("\n");
        LOGGER.info("SYNC edge Array - " + syncEdgeCountLog.toString());
        LOGGER.info("\n");
        LOGGER.info("SYNC vertex Array - " + syncVertexCountLog.toString());
        LOGGER.info("\n");
        LOGGER.info("SYNC triangle Array - " + syncTriangleCountLog.toString());
        LOGGER.info("\n");
        LOGGER.info("SYNC HD Set Array - " + syncHDSetSize.toString());
        LOGGER.info("\n");
        LOGGER.info("SYNC Triangle Set Size - " + syncTriangleSetSize.toString());

    }

    /**
     * For every edge, verify if a triangle is identified on this Worker
     * @param v1
     * @param v2
     * @return
     */
    protected static List<Long> addEdgeAndCheckTriangles(long v1, long v2, byte threadId) {

        assert ((v1 != -1L) && (v2 != -1L) && (v1 != v2)) : "Received Vertices are INVALID";

        boolean foundEdge = false;

        TLongHashSet v1Neighbours =  new TLongHashSet();
        TLongHashSet v2Neighbours =  new TLongHashSet();

//         DEFINE INTERSECTION SET
        List<Long> intersectionSet;
         IntersectHashSetFinelock intersectHashSetFinelock;

        long queuingTimeStart = System.currentTimeMillis();

        synchronized (partitionAdjacencyListLock) {

            totalWaitTimeBeforeCriticalSection.addAndGet((System.currentTimeMillis()-queuingTimeStart));
            long triangleCountCriticalSectionStart = System.currentTimeMillis();
            // ----------- TIME --------------------
            long getStart = System.currentTimeMillis();
            TLongHashSet adjacencyList = partitionAdjacencyList.get(v1);
            TLongHashSet adjacencyList2 = partitionAdjacencyList.get(v2);
            long getEnd = System.currentTimeMillis();
            totalAdjacencyListGetTime.addAndGet((getEnd-getStart));

            // --------- TIME ------------------
            long putStart = System.currentTimeMillis();

            if (adjacencyList == null) {
                adjacencyList = new TLongHashSet();
                partitionAdjacencyList.put(v1, adjacencyList);
            }


            if (adjacencyList2 == null) {
                adjacencyList2 = new TLongHashSet();
                partitionAdjacencyList.put(v2, adjacencyList2);
            }

            long putEnd = System.currentTimeMillis();
            totalAdjacencyListPutTime.addAndGet((putEnd-putStart));

            // -------- TIME -----------------
            long addStart = System.currentTimeMillis();
           if(v1<v2) { // ALWAYS get lock on smaller vertex ID before larger to avoid deadlocks
                synchronized(adjacencyList) {
                    synchronized(adjacencyList2) {
                        adjacencyList.add(v2);
                        adjacencyList2.add(v1);                                
                    }
                } 
            } else {
                    synchronized(adjacencyList2) {
                        synchronized(adjacencyList) {
                            adjacencyList.add(v2);
                            adjacencyList2.add(v1);                                
                        }
                    }
            }
            long addEnd = System.currentTimeMillis();
            totalHashSetAddTime.addAndGet((addEnd-addStart));

            long addAllStart = System.currentTimeMillis();
            intersectHashSetFinelock = new IntersectHashSetFinelock(adjacencyList, adjacencyList2, v1, v2);
            long addAllEnd = System.currentTimeMillis();
            totalAddAllTime.addAndGet((addAllEnd-addAllStart));


            long triangleCountCriticalSectionEnd = System.currentTimeMillis();

            totalTriangleCountCriticalSectionTime.addAndGet((triangleCountCriticalSectionEnd-triangleCountCriticalSectionStart));

            if (syncEdgeBoundaryPending.get()) {
                if((v1 == syncEdgeBoundarySource && v2 == syncEdgeBoundarySink) || (v2 == syncEdgeBoundarySource && v1 == syncEdgeBoundarySink) ) {
                    LOGGER.info(Thread.currentThread().getName() + " Found Boundary Edge  - " + v1 + "," + v2);
                    foundEdge = true;
                }
            }
        }

        totalNeighbourCount.addAndGet(intersectHashSetFinelock.getSum());
        setCounter.addAndGet(2);

        totalComparisonCount.addAndGet(intersectHashSetFinelock.getProduct());
        edgeSetCounter.addAndGet(1);
        // here -> (summed) up
            //Identify the third vertex of every triangle by intersection of adjacency linked lists
        //-------- TIME ---------- //;
        long retainAllStart= System.currentTimeMillis();
        intersectionSet = intersectHashSetFinelock.intersect();
        long retainAllEnd = System.currentTimeMillis();
        totalRetainAllTime.addAndGet((retainAllEnd-retainAllStart));
        retainAllCount.addAndGet(1);


        long remainingTimeStart= System.currentTimeMillis();

            if (!intersectionSet.isEmpty()) {
                int setSize = intersectionSet.size();
                numTrianglesIdentified.addAndGet((setSize));
                intersectionSet.add(v1);
                intersectionSet.add(v2);
            }

            assert ((v1Neighbours.isEmpty()) || (v1Neighbours.contains(v1) && v1Neighbours.contains(v2))) :
                    "Upon insertion of edge Triangle Set is neither empty nor contains v1 and v2";


            long remainingTimeEnd= System.currentTimeMillis();
            totalRemainingTime.addAndGet((remainingTimeEnd-remainingTimeStart));
            totalTriangleCountNonCriticalSectionTime.addAndGet((remainingTimeEnd-retainAllStart));

            if (foundEdge) {
                synchronized (syncStateLock) {
                    if (!syncBoundaryNotification.compareAndSet(false, true)) {
                        LOGGER.info(Thread.currentThread().getName() + " -  Notifying sync Thread ");
                        syncStateLock.notify();
                    }
                }
            }


        return intersectionSet;
    }

    private void trianglePrinter(TLongHashSet vertexIntersectionSet, long v1, long v2) {
			for(TLongIterator it = vertexIntersectionSet.iterator(); it.hasNext();) {
				long thirdVertex = it.next();
				//Format is v1, v2, v3. v1 and v2 are the vertices of the edge
				LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",WORKER.TRIANGLE_DISCOVERED," + v1 + "," + v2 + "," + thirdVertex);
			}
    }

    /**
     * Identify all vertices that are high degree on this partition
     * after receiving some fixed count edges from reservoir
     * @param avgGlobalDeg
     * @return map of vertices that are star centers in this partition
     */
    private ByteBuffer findHighDegreeVerticesByteStream(int avgGlobalDeg) {

        List<Long> hdVertices = new ArrayList<>();
        long[] adjListKeys;
        TLongHashSet neighboursList;

         synchronized (partitionAdjacencyListLock) {
            adjListKeys = partitionAdjacencyList.keys();
        }

        for (long src: adjListKeys) {

             neighboursList = partitionAdjacencyList.get(src);

            int neighbourListSize = neighboursList.size();

            if (neighbourListSize > (2 * avgGlobalDeg)) {
                long vertexPartitionBitMask = 0;
                vertexPartitionBitMask =(long) neighbourListSize;
                vertexPartitionBitMask = vertexPartitionBitMask << 40;
                vertexPartitionBitMask |= src;

                hdVertices.add(vertexPartitionBitMask);
            }
        }

        ByteBuffer hdVerticesByteBuffer = ByteBuffer.allocate(hdVertices.size()*8);

        for (long vertex : hdVertices) {
            hdVerticesByteBuffer.putLong(vertex);
        }

        //Set the marker to the start before sending to the Master
        hdVerticesByteBuffer.rewind();

        return hdVerticesByteBuffer;
    }

    /**
     * Based on the color of the received edge, identify the red vertices
     * Update the existing sets of red vertices and place in appropriate sets
     *
     * @param v1
     * @param v2
     * @param edgeColor
     */
     private void colorChecker(long v1, long v2, byte edgeColor) {

        // This is the home of v1 for edge (v1,v2) and v2 is replicated here
        if (edgeColor == 1) {
            if (!partitionAdjacencyList.containsKey(v2)) {
                // If v2 has not been seen before, add it to set red1
                red1Vertices.add(v2);
                red1VertexCount++;
            } else {
                assert(partitionAdjacencyList.containsKey(v2)) :
                        "v2 is not identified as present even though atleast one edge has been seen for it.";

                if (red1Vertices.contains(v2)) {
                    // If v2 has been seen before in red1 then
                    // it was part of some edge (vx,v2) earlier and this is second entry
                    // remove v2 from red1 set
                    red1Vertices.remove(v2);
                }

                if (red1Vertices.contains(v1)) {
                    // If v1 has been seen before in red1 then
                    // it was part of some edge (vx,v1) earlier and this is second entry but as (v1,v2)
                    // remove v1 from red1 set

                    assert(partitionAdjacencyList.containsKey(v1)) :
                            "v1 is not identified as present even though atleast one edge has been seen for it.";
//					LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",WORKER.DEBUG," + "REMOVE_RED_VERTEX_1,V1," + v1);
                    red1Vertices.remove(v1);
                }

                if (red2Vertices.contains(v2)) {
                    // If v2 has been seen before in red2 then
                    // it was part of some edge (v2,vx) earlier
                    // and it was sent here as (vx,v2)
                    // now edge itself is (v1,v2)
                    // remove v2 from red2 set as new edge has come for it
                    red2Vertices.remove(v2);
                }

                if (red2Vertices.contains(v1)) {
                    // If v1 has been seen before in red2 then
                    // it was part of some edge (v1,vx) earlier
                    // and it was sent here as (vx,v1)
                    // now edge itself is (v1,v2)
                    // remove v1 from red2 set as new edge has come for it

                    assert(partitionAdjacencyList.containsKey(v1)) :
                            "v1 is not identified as present even though atleast one edge has been seen for it.";
                    red2Vertices.remove(v1);
                }

                // All these cases mean edge is now green
                greenEdgeCount++;
            }

        } else if (edgeColor == 2) {
            // This is the home of v2 for edge (v1,v2) and v1 is replicated here
            if (!partitionAdjacencyList.containsKey(v2)) {
                // v2's home where it forms triangle is another partition
                // check if it also present here. If not, add it to red2 set
                red2Vertices.add(v2);
                red2VertexCount++;
            } else {

                assert(partitionAdjacencyList.containsKey(v2)) :
                        "v2 is not identified as present even though atleast one edge has been seen for it.";

                if (red1Vertices.contains(v2)) {
                    // If v2 has been seen before in red1 then
                    // it was part of some edge (vx,v2) earlier and this is second entry
                    // remove v2 from red1 set
                    red1Vertices.remove(v2);
                }

                if (red1Vertices.contains(v1)) {
                    // If v1 has been seen before in red1 then
                    // it was part of some edge (vx,v1) earlier and this is second entry
                    // remove v1 from red1 set

                    assert(partitionAdjacencyList.containsKey(v1)) :
                            "v1 is not identified as present even though atleast one edge has been seen for it.";
                    red1Vertices.remove(v1);
                }

                if (red2Vertices.contains(v1)) {
                    // If v1 has been seen before in red2 then
                    // it was part of some edge (vx,v1) earlier and its home was where (v1, vx) was sent
                    // this is second entry
                    // remove v1 from red1 set

                    assert(partitionAdjacencyList.containsKey(v1)) :
                            "v1 is not identified as present even though atleast one edge has been seen for it.";

                    red2Vertices.remove(v1);
                }

                if (red2Vertices.contains(v2)) {
                    // If v2 has been seen before in red2 then
                    // it was part of some edge (vx,v2) earlier and this is second entry
                    // remove v2 from red2 set
                    red2Vertices.remove(v2);
                }

                greenEdgeCount++;
            }

        } else {
            // edge is green. Update the sets if either of the vertices are present in red sets
            if (red1Vertices.contains(v2)) {
                red1Vertices.remove(v2);
            }

            if (red1Vertices.contains(v1)) {
                assert(partitionAdjacencyList.containsKey(v1)) :
                        "v1 is not identified as present even though atleast one edge has been seen for it.";

                red1Vertices.remove(v1);
            }

            if (red2Vertices.contains(v1)) {
                assert(partitionAdjacencyList.containsKey(v1)) :
                        "v1 is not identified as present even though atleast one edge has been seen for it.";

                red2Vertices.remove(v1);
            }

            if (red2Vertices.contains(v2)) {
                red2Vertices.remove(v2);
            }

            greenEdgeCount++;
        }

    }

    public void edgeListHandler(MessageType messageType, List<String> edges, byte edgeColor) {

        for (String edge : edges) {
            String[] vertices = edge.split(",");
            long v1, v2;
            try {
                v1 = Long.parseLong(vertices[0]);
            } catch (NumberFormatException e) {
                v1 = -1L;
                e.printStackTrace();
            }

            try {
                v2 = Long.parseLong(vertices[1]);
            } catch (NumberFormatException e) {
                v2 = -1L;
                e.printStackTrace();
            }

            assert ((v1 != -1L) && (v2 != -1L) && (v1 != v2)) : "Vertices read from Queue are INVALID";
            edgeHandler(v1, v2, edgeColor);
        }
    }

    public void edgeHandler(long v1, long v2, byte edgeColor) {

           int count = totalEdgeCount.incrementAndGet();
        // first edge recvd (start)
          if (count == 1) {
              startTime = System.currentTimeMillis();
          }

          if (syncResponseSent.get()) {
              time_between_sent_response_and_new_edge_arrival.add((System.currentTimeMillis() - syncSentTimestamp.get()));
              syncResponseSent.set(false);
          }

          Edge edge = new Edge(v1, v2);

          long taskCreationStart  =System.currentTimeMillis();
          EdgeTask task = new EdgeTask(edge);
          long taskCreationEnd = System.currentTimeMillis();

          taskCreationTime.addAndGet((taskCreationEnd-taskCreationStart));


          long taskExecutionStart = System.currentTimeMillis();
          executor.execute(task);
          long taskExecutionEnd = System.currentTimeMillis();

          if (count % 10000 == 0) {
            MultiThreadedWorker.batch_timestamps.add(System.currentTimeMillis());
            MultiThreadedWorker.batch_total_received_edges.add(count);
            MultiThreadedWorker.batch_total_processed_edges.add(MultiThreadedWorker.totalProcessedEdgeCount.get());
          }

          taskExecutionTime.addAndGet((taskExecutionEnd-taskExecutionStart));
    }

    public Map<String, ByteBuffer> syncHandler(MessageType messageType, int avgGlobalDeg, long v1, long v2) {
        LOGGER.info("--- Entering Sync Handler ------");

        long syncStart = System.currentTimeMillis();
        Map<String,ByteBuffer> specialVertices = new HashMap<>();
        TLongHashSet triangleVertToSendCopy;
        int totalVertices = 0;
        ConcurrentHashMap<Long,Boolean> triangleMapCopy = new ConcurrentHashMap<Long,Boolean>(1000000, 0.75f, MultiThreadedWorkerServer.N_THREADS+1);
        ConcurrentHashMap.KeySetView<Long,Boolean> triSet2 = triangleMapCopy.keySet(Boolean.TRUE);

        if (v1 != -1 && v2 != -1) {
            long waitForLockTimeStart = System.currentTimeMillis();
            synchronized (partitionAdjacencyListLock) {
                long waitForLockTimeEnd = System.currentTimeMillis();
                per_sync_wait_time_to_acquire_lock.add(waitForLockTimeEnd-waitForLockTimeStart);

                TLongHashSet v1Neighbours = null;
                TLongHashSet v2Neighbours = null;
                totalVertices = partitionAdjacencyList.size();

                long getFromPartitionsStart = System.currentTimeMillis();
    
                v1Neighbours = partitionAdjacencyList.get(v1);
                v2Neighbours = partitionAdjacencyList.get(v2);

                if (v1Neighbours != null && v2Neighbours != null) {
                    // TODO:YS: If using fine grained locking of adj list, need to lock here as well.
                    
                    if(v1<v2) { // ALWAYS get lock on smaller vertex ID before larger to avoid deadlocks
                        synchronized(v1Neighbours) {
                            synchronized (v2Neighbours) {
                                if (!v1Neighbours.contains(v2) && !v2Neighbours.contains(v1)) {                                    
                                    syncEdgeBoundarySource = v1;
                                    syncEdgeBoundarySink = v2;                                 
                                    syncEdgeBoundaryPending.set(true);   
                                    LOGGER.info("Boundary Edge - " + syncEdgeBoundarySource + "," + syncEdgeBoundarySink);
                                }
                            }
                        }
                    } else {
                            synchronized(v2Neighbours) {
                                synchronized(v1Neighbours) {
                                    if (!v1Neighbours.contains(v2) && !v2Neighbours.contains(v1)) {
                                        syncEdgeBoundarySource = v1;
                                        syncEdgeBoundarySink = v2;                                 
                                        syncEdgeBoundaryPending.set(true);   
                                        LOGGER.info("Boundary Edge - " + syncEdgeBoundarySource + "," + syncEdgeBoundarySink);
                                    }
                                }
                            }
                    }

                }
                long getFromPartitionsEnd = System.currentTimeMillis();
                per_sync_get_from_partitions.add((getFromPartitionsEnd-getFromPartitionsStart));
            }

            long waitStart = System.currentTimeMillis();
            if (syncEdgeBoundaryPending.get()) {
                synchronized (syncStateLock) {
                    if (syncBoundaryNotification.compareAndSet(false, true)) {
                        try {
                            LOGGER.info("Waiting for boundary edge to be processed - " + syncEdgeBoundarySource + "," + syncEdgeBoundarySink);
                            syncStateLock.wait();
                        } catch (InterruptedException ex) {
                            LOGGER.info(Thread.currentThread().getId() + " Interruped while waiting for sync");
                            ex.printStackTrace();
                        }
                    }
                    syncBoundaryNotification.set(false);
                }
            }
            long waitEnd = System.currentTimeMillis();
            per_sync_boundary_wait.add((waitEnd-waitStart));
            syncEdgeBoundaryPending.set(false);
            syncEdgeBoundarySource = -1;
            syncEdgeBoundarySink = -1;
        }

        long syncTrOpStart = System.currentTimeMillis();
        List<Long> mqcVerticesToReturn = new ArrayList<>();

        MultiThreadedWorkerServer.triSetReadWriteLock.writeLock().lock();
        ConcurrentHashMap.KeySetView<Long,Boolean> placeholder = MultiThreadedWorkerServer.triSet;
        MultiThreadedWorkerServer.triSet = triSet2;
        triSet2 = placeholder;
        MultiThreadedWorkerServer.triSetReadWriteLock.writeLock().unlock();

        
        // operate on triSet2 for sending sync triangles
        for(Iterator<Long> it = triSet2.iterator(); it.hasNext();) {
            long vertex = it.next();
            if (!triangleVertexSet.contains(vertex)) {
                mqcVerticesToReturn.add(vertex);
                triangleVertexSet.add(vertex);
            }
        }
        

        ByteBuffer mqcVerticesByteStream = ByteBuffer.allocate(mqcVerticesToReturn.size()*8);
        ByteBuffer starVerticesByteStream;

        for(long vertex: mqcVerticesToReturn) {
            mqcVerticesByteStream.putLong(vertex);
        }

        specialVertices.put("mqc", mqcVerticesByteStream);

        //Set the marker to the start before sending to the Master
        mqcVerticesByteStream.rewind();
        long syncTrOpEnd = System.currentTimeMillis();
        per_sync_triangle_op.add((syncTrOpEnd-syncTrOpStart));

        long syncHdOpStart = System.currentTimeMillis();
        if ((heuristic == MultiThreadedHeuristic.B) || (heuristic == MultiThreadedHeuristic.BT)) {
            /*Send empty set if the above heuristics are used */
            starVerticesByteStream = ByteBuffer.allocate(0);
            specialVertices.put("star", starVerticesByteStream);
        } else {
            /*Identify high degree vertices for all other heuristics */
            starVerticesByteStream = findHighDegreeVerticesByteStream(avgGlobalDeg);
            specialVertices.put("star", starVerticesByteStream);
        }

        //Set the marker to the start before sending to the Master
        starVerticesByteStream.rewind();
        long syncHdOpEnd = System.currentTimeMillis();
        per_sync_hd_op.add((syncHdOpEnd-syncHdOpStart));

        int hdSetSize = starVerticesByteStream.capacity()/8;
        int triangleSetSize = mqcVerticesToReturn.size();

        LOGGER.info("TIME_MS," + System.currentTimeMillis() + ",AT_SYNC_RESPONSE,WORKER.CURRENT_HDSET_SIZE," + hdSetSize +
                ",AT_SYNC_RESPONSE,WORKER.CURRENT_TRIANGLESET_SIZE," +	triangleSetSize + ",WORKER.COMMON_VERTICES," + 0 +
                ",WORKER.AVG_DEGREE_RECEIVED," + avgGlobalDeg + ",WORKER.TOTAL_TRIANGLES," + numTrianglesIdentified.get() + " ,WORKER.TOTAL_EDGES," +
                totalEdgeCount.get() + ",WORKER.TOTAL_VERTICES," + totalVertices);
        LOGGER.info("TOTAL PROCESSED EDGE COUNT - " + totalProcessedEdgeCount.get() + ", TOTAL EDGE COUNT RECIEVED - " + totalEdgeCount);

        syncEdgeCountLog.add(totalProcessedEdgeCount.get());
        syncTriangleCountLog.add(numTrianglesIdentified.get());
        syncVertexCountLog.add(totalVertices);
        syncHDSetSize.add(hdSetSize);
        syncTriangleSetSize.add(triangleSetSize);

        long syncEnd = System.currentTimeMillis();
        totalSyncTime.addAndGet((syncEnd-syncStart));
        perSyncTime.add((syncEnd-syncStart));
        syncSentTimestamp.set(System.currentTimeMillis());
        syncResponseSent.set(true);
        long counter = setCounter.get();
        if (counter != 0) {
            per_sync_avg_neighbours_per_vertex.add(totalNeighbourCount.get()/setCounter.get());
        } else {
            per_sync_avg_neighbours_per_vertex.add(0L);
        }

        long edgeCounter = edgeSetCounter.get();
        if (edgeCounter != 0) {
            per_sync_avg_comparisons_per_edge.add(totalComparisonCount.get()/edgeCounter);
        } else {
            per_sync_avg_comparisons_per_edge.add(0L);
        }

        return specialVertices;
    }

    public void connectionClosing(MessageType messageType) {
    }

}
