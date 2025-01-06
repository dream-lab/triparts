package in.dreamlab.MultiThreadedPartitioning;

import com.google.common.hash.BloomFilter;
import com.google.common.util.concurrent.AtomicDouble;
import gnu.trove.iterator.TByteIterator;
import gnu.trove.map.hash.TByteIntHashMap;
import gnu.trove.set.TByteSet;
import gnu.trove.set.hash.TByteHashSet;
import in.dreamlab.MultiThreadedPartitioning.thrift.MessageType;
import in.dreamlab.MultiThreadedPartitioning.thrift.MultiThreadedWorkerService;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class PartitionerThread implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionerThread.class);
    private ConcurrentLinkedQueue<Edge> IOQueue;
    private ConcurrentLinkedQueue<Edge>[] transportQueues;
    private static Long processedEdgeCount = new Long(0);

    // PERF METRICS VARIABLES
    private long totalIOQueueReadTime = 0L;
    private long totalQueuePutTime = 0L;
    private long totalDecisionTime = 0L;
    private long totalSyncWaitTime = 0L;
    private long totalIOSyncWaitTime = 0L;

    protected static Object TMapLock = new Object();
    protected static float globalAvgDegree = 0;
    protected static float newAvgDegree = 0;
    protected static int AVG_DEGREE_UPDATE_COUNT = 10000;

    public PartitionerThread(ConcurrentLinkedQueue<Edge> IOQueue, ConcurrentLinkedQueue<Edge>[] transportQueues) {
        this.IOQueue = IOQueue;
        this.transportQueues = transportQueues;
    }

    private void incrementHitRate(int index) {
        MultiThreadedMaster.hitRate[index].incrementAndGet();
    }

    private byte getLightestPartition() {
        long getLightestPartitionStart = System.currentTimeMillis();
        byte lightestPartition = 0;
        for (byte i = 1; i < MultiThreadedMaster.currentPartitionSizes.length; ++i) {
            if (MultiThreadedMaster.currentPartitionSizes[i].get() <= MultiThreadedMaster.currentPartitionSizes[lightestPartition].get()
                    && (MultiThreadedMaster.currentPartitionSizes[i].get() < MultiThreadedMaster.lowerThreshold)) {
                lightestPartition = i;
            }
        }
        long getLightestPartitionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getLightestPartition.addAndGet((getLightestPartitionEnd-getLightestPartitionStart));
        return lightestPartition;
    }

    /** Identify the partitions on which vertex is present
     * @param vertex
     * @return set of partitions on which vertex is present
     * */
    private TByteHashSet getVertexBFSet(Long vertex) {
        long getVertexBFSetStart = System.currentTimeMillis();
        TByteHashSet vertexReplicaPartitions = new TByteHashSet(MultiThreadedMaster.numPartitions);

        for (byte i = 0; i < MultiThreadedMaster.bloomFilterArray.length; i++) {
            if (MultiThreadedMaster.bloomFilterArray[i].mightContain(vertex)) {
                vertexReplicaPartitions.add(i);
            }
        }
        long getVertexBFSetEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getVertexBFSet.addAndGet((getVertexBFSetEnd-getVertexBFSetStart));
        return vertexReplicaPartitions;
    }

    /**
     * Identify partition where either vertex has a higher degree from sets. Compare and return the max degree partition
     * @param v1
     * @param v2
     * @param v1PartitionSet
     * @param v2PartitionSet
     * @return
     */
    private byte getPartitionOfMaxDegreeFromTwoSets(long v1, long v2, TByteSet v1PartitionSet, TByteSet v2PartitionSet) {
        long getPartitionOfMaxDegreeFromTwoSetsStart = System.currentTimeMillis();
        /*
         * Find partition when both exist on different sets and we need the partition which has the max degree of the vertex replica
         * between either set
         * */
        byte partition = -1;
        int v1MaxDeg = 0;
        byte v1Partition = -1;
        int v2MaxDeg = 0;
        byte v2Partition = -1;

        TByteIntHashMap v1PartitionDegMap;
        TByteIntHashMap v2PartitionDegMap;

        byte v1PartitionIterator, v2PartitionIterator;
        int v1CurrentDeg, v2CurrentDeg;

        if (v1PartitionSet != null && !v1PartitionSet.isEmpty()) {

            TByteIterator it1 = v1PartitionSet.iterator();
            v1PartitionDegMap = MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1);

            while(it1.hasNext()) {
                v1PartitionIterator = it1.next();


                v1CurrentDeg = v1PartitionDegMap.get(v1PartitionIterator);

                if (v1CurrentDeg > v1MaxDeg) {
                    // if the degree in the partition is greater than the current max, set the partition and the max degree
                    v1MaxDeg = v1CurrentDeg;
                    v1Partition = v1PartitionIterator;
                } else if (v1CurrentDeg == v1MaxDeg) {
                      /*
                     * this will never be evaluated in the first iteration because the init value of maxDegree is 0
                     * so for second partition in the set, change partitionID only if lower partitionID is found, else let it be
                     * */

                    if (v1PartitionIterator < v1Partition) {
                        // always find the lowest partition ID and reset only if lower partitionID is found
                        v1Partition = v1PartitionIterator;
                    }
                }
            }
        }

        if (v2PartitionSet != null && !v2PartitionSet.isEmpty()) {

            TByteIterator it2 = v2PartitionSet.iterator();
            v2PartitionDegMap = MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2);

            while(it2.hasNext()) {
                v2PartitionIterator = it2.next();


                v2CurrentDeg = v2PartitionDegMap.get(v2PartitionIterator);
                if (v2CurrentDeg > v2MaxDeg) {
                    // if the degree in the partition is greater than the current max, set the partition and the max degree
                    v2MaxDeg = v2CurrentDeg;
                    v2Partition = v2PartitionIterator;
                } else if (v2CurrentDeg == v2MaxDeg) {
                    //					LOGGER.info("SAME DEGREE SUM IN getPartitionOfMaxDegreeFromTwoSets:" + v2PartitionIterator + "," + v2Partition + ",v1:" + v1 + ",v2:" + v2);
                    /*
                     * this will never be evaluated in the first iteration because the init value of maxDegree is 0
                     * so for second partition in the set, change partitionID only if lower partitionID is found, else let it be
                     * */
                    if (v2PartitionIterator < v2Partition) {
                        // always find the lowest partition ID and reset only if lower partitionID is found
                        v2Partition = v2PartitionIterator;
                    }
                }
            }
        }

        if (v1Partition != -1 && v2Partition != -1) {
            if (v1MaxDeg >= v2MaxDeg) {
                partition = v1Partition;
            } else {
                partition = v2Partition;
            }
        } else if (v1Partition != -1) {
            partition = v1Partition;
        } else if (v2Partition != -1) {
            partition = v2Partition;
        } else {
            partition = -1;
        }

        if ((partition != -1) && MultiThreadedMaster.currentPartitionSizes[partition].get() >= MultiThreadedMaster.higherThreshold) {
            /* There is possibility of getting a partition which doesn't have enough space
             * Eg- 0 amd 1 have equal degree for v1 and higher than deg for whatever partition v2 is in
             * 0 is chosen but it doesn't have space.
             * I get determinism by trading off quality
             * */
            partition = -1;
        }
        long getPartitionOfMaxDegreeFromTwoSetsEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getPartitionOfMaxDegreeFromTwoSets.addAndGet((getPartitionOfMaxDegreeFromTwoSetsEnd-getPartitionOfMaxDegreeFromTwoSetsStart));
        return partition;
    }

    /**
     * Identify lightest partition in the given set
     * @param partitionSet
     * @param maxCurrentCapacity
     * @return
     */
    // TODO - time this for currentPartitionSizes
    private byte getLeastLoadedPartitionInSet(TByteHashSet partitionSet, int maxCurrentCapacity, long v1, long v2) {
        long getLeastLoadedPartitionStart = System.currentTimeMillis();
        byte partition = -1;

        if (partitionSet == null || partitionSet.isEmpty()) {
            return -1;
        }

        TByteIterator it = partitionSet.iterator();
        byte lightestPartition = it.next();

        while (it.hasNext()) {
            byte partitionIterator = it.next();

            if ((MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() <= MultiThreadedMaster.currentPartitionSizes[lightestPartition].get())
                    && (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() < maxCurrentCapacity)) {

                if (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() == MultiThreadedMaster.currentPartitionSizes[lightestPartition].get()) {

                    //LOGGER.info("SAME CURRENT WT IN getLeastLoadedPartitionInSet:" + partitionIterator + "," + lightestPartition + ",v1:" + v1 + ",v2:" + v2);
                    if (partitionIterator < lightestPartition) {
                        lightestPartition = partitionIterator;
                    } else {
                        continue;
                    }
                } else {
                    lightestPartition = partitionIterator;
                }

            }
        }

        if ((MultiThreadedMaster.currentPartitionSizes[lightestPartition].get() < maxCurrentCapacity)) {
            //A partition ID can definitely be assigned to the edge
            partition = lightestPartition;
        }
        long getLeastLoadedPartitionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getLeastLoadedPartition.addAndGet((getLeastLoadedPartitionEnd-getLeastLoadedPartitionStart));
        return partition;
    }

    /**
     * Identify the partition from set of partitions where vertex has highest degree
     * @param vertexID
     * @param partitionSet
     * @return
     */
    private byte getPartitionOfMaxDegreeVertexFromSet(long vertexID, long v2, TByteSet partitionSet) {
        long getPartitionOfMaxDegreeVertexFromSetStart = System.currentTimeMillis();

        byte partition = -1;
        int maxDegree = 0;
        byte partitionIterator;
        int currentDeg;

        TByteIntHashMap v1PartitionDegMap;

        if (partitionSet == null || partitionSet.isEmpty()) {
            return -1;
        }

        TByteIterator it = partitionSet.iterator();
        v1PartitionDegMap = MultiThreadedMaster.vertexHDPartitionDegreeMap.get(vertexID);

        while(it.hasNext()) {
            partitionIterator = it.next();


            currentDeg = v1PartitionDegMap.get(partitionIterator);

            if (currentDeg > maxDegree) {
                // if the current deg is greater than the current max, set the partition and the max degree
                maxDegree = currentDeg;
                partition = partitionIterator;

            } else if (currentDeg == maxDegree) {
                //				LOGGER.info("SAME DEGREE VALUE IN getPartitionOfMaxDegreeVertexFromSet:" + partitionIterator + "," + partition + ",vertexID:" + vertexID + ",otherV:" + v2);
                /*
                 * this will never be evaluated in the first iteration because the init value of maxDegree is 0
                 * so for second partition in the set, change partitionID only if lower partitionID is found, else let it be
                 * */
                if (partitionIterator < partition) {
                    // always find the lowest partition ID and reset only if lower partitionID is found
                    partition = partitionIterator;
                }
            }
        }

        if (MultiThreadedMaster.currentPartitionSizes[partition].get() >= MultiThreadedMaster.higherThreshold) {
            partition = -1;
        }
        long getPartitionOfMaxDegreeVertexFromSetEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getPartitionOfMaxDegreeVertexFromSet.addAndGet((getPartitionOfMaxDegreeVertexFromSetEnd-getPartitionOfMaxDegreeVertexFromSetStart));
        return partition;
    }

    /**
     * Identify partition from Intersection of partition sets where sum of v1 degree and v2 degree is maximized
     * @param v1
     * @param v2
     * @param intersectionSet
     * @return
     */
    private byte getMaxDegreeSumPartitionFromSet(long v1, long v2, TByteSet intersectionSet) {
        long getMaxDegreeSumPartitionStart = System.currentTimeMillis();
        /*
         * find sum of degree for both vertices when both in HD Map and intersection is not null
         * */
        byte partition = -1;
        int sumOfDegrees = 0;
        int maxDegree = 0;
        int v1Deg = 0;
        int v2Deg = 0;
        byte partitionIterator;


        if (intersectionSet == null || intersectionSet.isEmpty()) {
            return -1;
        }

        TByteIntHashMap v1PartitionDegMap = MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1);
        TByteIntHashMap v2PartitionDegMap = MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2);

        TByteIterator it = intersectionSet.iterator();

        while (it.hasNext()) {
            partitionIterator = it.next();
            v1Deg = v1PartitionDegMap.get(partitionIterator);
            v2Deg = v2PartitionDegMap.get(partitionIterator);
            sumOfDegrees = v1Deg + v2Deg; //sum of degrees of v1 and v2 on the two partitions

            if (sumOfDegrees > maxDegree) {
                // if the sum is greater than the current max, set the partition and the max degree
                partition = partitionIterator;
                maxDegree = sumOfDegrees;
            } else if (sumOfDegrees == maxDegree) {
                //				LOGGER.info("SAME DEGREE SUM IN getMaxDegreeSumPartitionFromSet:" + partitionIterator + "," + partition + ",v1:" + v1 + ",v2:" + v2);
                /*
                 * this will never be evaluated in the first iteration because the init value of maxDegree is 0
                 * so for second partition in the set, change partitionID only if lower partitionID is found, else let it be
                 * */
                if (partitionIterator < partition) {
                    // always find the lowest partition ID and reset only if lower partitionID is found
                    partition = partitionIterator;
                }
            }
        }

        if (MultiThreadedMaster.currentPartitionSizes[partition].get() >= MultiThreadedMaster.higherThreshold) {
            partition = -1;
        }
        long getMaxDegreeSumPartitionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getMaxDegreeSumPartitions.addAndGet((getMaxDegreeSumPartitionEnd-getMaxDegreeSumPartitionStart));
        return partition;
    }

    /**
     * Identify lightest partition in intersection of two partition sets[either or both could be empty]
     * @param v1PartSet
     * @param v2PartSet
     * @return
     */
    private byte getPartitionOfIntersectionSets(TByteSet v1PartSet, TByteSet v2PartSet) {
        long partitionIntersectionSetsStart = System.currentTimeMillis();
        byte partition = -1;

        // Identify the common partitions between the two vertices
        TByteSet intersectionPartitionSet = new TByteHashSet(v1PartSet);
        intersectionPartitionSet.retainAll(v2PartSet);

        if (!intersectionPartitionSet.isEmpty()) {
            /*Both vertices have >=1 common partition*/
            TByteIterator it = intersectionPartitionSet.iterator();
            byte lightestCommonPartition = it.next();

            while (it.hasNext()) {
                byte partitionIterator = it.next();

                if ((MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() <=
                        MultiThreadedMaster.currentPartitionSizes[lightestCommonPartition].get())
                        && (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() <
                        MultiThreadedMaster.higherThreshold)) {
                    if (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() ==
                            MultiThreadedMaster.currentPartitionSizes[lightestCommonPartition].get()) {
                        if (partitionIterator < lightestCommonPartition) {
                            lightestCommonPartition  = partitionIterator;
                        }
                    } else {
                        lightestCommonPartition = partitionIterator;
                    }
                }
            }
            if (MultiThreadedMaster.currentPartitionSizes[lightestCommonPartition].get() < MultiThreadedMaster.higherThreshold) {
                //A partition ID can definitely be assigned to the edge
                partition = lightestCommonPartition;
            }
        }
        long partitionIntersectionSetsEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getPartitionOfIntersectionSets.addAndGet((partitionIntersectionSetsEnd-partitionIntersectionSetsStart));
        return partition;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Finds the intersection of items between two sets and returns a new set with the common items in both.
     * If either lset not rset are NULL or empty, returns an empty set.
     * @param lset
     * @param rset
     * @return
     */
    private static TByteHashSet setIntersection(TByteHashSet lset, TByteHashSet rset){
        long twoSetIntersectionStart = System.currentTimeMillis();
        // boundary check
        if(lset == null || rset == null) return new TByteHashSet(0);
        int lsize = lset.size();
        int rsize = rset.size();
        if(lsize == 0 || rsize == 0) return new TByteHashSet(0);

        TByteHashSet smallerSet = lsize < rsize ? lset : rset;
        TByteHashSet largerSet = lsize < rsize ? rset : lset;
        TByteHashSet interSet = new TByteHashSet(smallerSet.size());


        TByteIterator smallerIter = smallerSet.iterator();
        while (smallerIter.hasNext()) {
            byte val = smallerIter.next();
            if (largerSet.contains(val)) {
                interSet.add(val);
            }
        }
        long twoSetIntersectionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_TwoSetIntersectionTime.addAndGet((twoSetIntersectionEnd-twoSetIntersectionStart));
        return interSet;
    }

    /**
     * Finds the intersection of items between three sets and returns a new set with the common items in both.
     * If either lset not rset are NULL or empty, returns an empty set.
     * @param lset
     * @param rset
     * @return
     */
    private static TByteHashSet setIntersection(TByteHashSet lset, TByteHashSet mset, TByteHashSet rset){
        long threeSetIntersectionStart = System.currentTimeMillis();
        // boundary check
        if(lset == null || mset == null || rset == null) return new TByteHashSet(0);
        int lsize = lset.size();
        int msize = lset.size();
        int rsize = rset.size();
        if(lsize == 0 || msize == 0 || rsize == 0) return new TByteHashSet(0);


        TByteHashSet smallerSet;
        TByteHashSet largerSet1;
        TByteHashSet largerSet2;
        TByteHashSet interSet;

        //FIXME: If >=2 sets are of equal sizes?
        if(lsize < msize && lsize < rsize) { // lset is smallest
            smallerSet = lset;
            largerSet1 = mset;
            largerSet2 = rset;
            interSet = new TByteHashSet(lsize);
        } else
        if(msize < lsize && msize < rsize) { // mset is smallest
            smallerSet = mset;
            largerSet1 = lset;
            largerSet2 = rset;
            interSet = new TByteHashSet(msize);
        }
        else {
             smallerSet = rset;
            largerSet1 = lset;
            largerSet2 = mset;
            interSet = new TByteHashSet(rsize);
        }

        TByteIterator smallerIter = smallerSet.iterator();
        while ( smallerIter.hasNext() ) {
            byte val = smallerIter.next();
            if (largerSet1.contains(val) && largerSet2.contains(val) ) {
                interSet.add(val);
            }
        }
        long threeSetIntersectionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_ThreeSetIntersectionTime.addAndGet((threeSetIntersectionEnd-threeSetIntersectionStart));
        return interSet;
    }

    private TByteHashSet setUnion(TByteHashSet lset, TByteHashSet rset) {
        long setUnionStart = System.currentTimeMillis();
        if((lset == null || lset.isEmpty()) && (rset == null || rset.isEmpty())) return new TByteHashSet(0);
        if(lset == null || lset.isEmpty()) return new TByteHashSet(rset);
        if(rset == null || rset.isEmpty()) return new TByteHashSet(lset);

        TByteHashSet unionSet = new TByteHashSet(lset.size()+rset.size());
        unionSet.addAll(lset);
        unionSet.addAll(rset);
        long setUnionEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_setUnionTime.addAndGet((setUnionEnd-setUnionStart));
        return unionSet;
    }


    /**
     * Identify lightest partition in union of two partition sets[either or both could be empty]
     * @param v1PartSet
     * @param v2PartSet
     * @return
     */
    private byte getPartitionOfUnionSets(TByteSet v1PartSet, TByteSet v2PartSet) {
        long unionTimeStart = System.currentTimeMillis();
        byte partition = -1;

        // Identify the common partitions between the two vertices
        TByteSet unionPartitionSet = new TByteHashSet(v1PartSet);
        unionPartitionSet.addAll(v2PartSet);

        if (!unionPartitionSet.isEmpty()) {
            /*Both vertices have >=1 common partition*/
            TByteIterator it = unionPartitionSet.iterator();
            byte lightestUnionPartition = it.next();

            while (it.hasNext()) {
                byte partitionIterator = it.next();
                if ((MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() <=
                        MultiThreadedMaster.currentPartitionSizes[lightestUnionPartition].get())
                        && (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() <
                        MultiThreadedMaster.higherThreshold)) {

                    if (MultiThreadedMaster.currentPartitionSizes[partitionIterator].get() ==
                            MultiThreadedMaster.currentPartitionSizes[lightestUnionPartition].get()) {
                        if (partitionIterator < lightestUnionPartition) {
                            lightestUnionPartition  = partitionIterator;
                        }
                    } else {

                        lightestUnionPartition = partitionIterator;
                    }
                }
            }
            if (MultiThreadedMaster.currentPartitionSizes[lightestUnionPartition].get() < MultiThreadedMaster.higherThreshold) {
                //A partition ID can definitely be assigned to the edge
                partition = lightestUnionPartition;
            }
        }
        long unionTimeEnd = System.currentTimeMillis();
        MultiThreadedMaster.compute_total_getPartitionUnionOfSets.addAndGet((unionTimeEnd-unionTimeStart));
        return partition;
    }

    private byte getPartition_HDNormDynamic_Set(long v1, long v2) {
        MultiThreadedMaster.decisionHandlerEdgeCount.incrementAndGet();

        byte partitionID = -1;
        boolean bloomFilterFlag = true;

        boolean partitionFound = false;

        /* Lists to store partitions for vertices in TMAP */
        TByteSet v1TrianglePartitionSet;
        TByteSet v2TrianglePartitionSet;

        /* Lists to store partitions for vertices in HDMAP but never used */
        TByteSet v1HDPartitionSet = new TByteHashSet();
        TByteSet v2HDPartitionSet = new TByteHashSet();

        /* Lists to store partitions for vertices in Bloom Filters */
        TByteSet v1ReplicaPartitionSet;
        TByteSet v2ReplicaPartitionSet;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.containsKey(v1)) {
            v1TrianglePartitionSet = new TByteHashSet(MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).keySet());
        } else {
            v1TrianglePartitionSet = new TByteHashSet();
        }

        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.containsKey(v2)) {
            v2TrianglePartitionSet = new TByteHashSet(MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).keySet());
        } else {
            v2TrianglePartitionSet = new TByteHashSet();
        }


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if ((v1TrianglePartitionSet.size() != 0) && (v2TrianglePartitionSet.size() != 0)) {
            /* Both vertices are present in TMAP with atleast 1 partition */
            assert (!v1TrianglePartitionSet.isEmpty() && !v2TrianglePartitionSet.isEmpty()) : "Atleast one of the TMAP entries has no "
                    + "list of partitions";

            bloomFilterFlag = false;

            TByteSet triangleIntersectionSet = new TByteHashSet(v1TrianglePartitionSet);

            triangleIntersectionSet.retainAll(v2TrianglePartitionSet);

            if (triangleIntersectionSet.size() != 0) {
                /* v1 and v2 on same partition(s) in TMAP
                 * 1. Check T(v1) intersect T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect T(v2) intersect H(v1) and T(v1) intersect T(v2) intersect H(v2). Choose partition which has highest degree vertex.
                 * 3. Check T(v1) intersect H(v2) intersect H(v1) and T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 4. Check lightest in T(v1) intersect T(v2)
                 * 5. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check union of BF(v1) and BF(v2)
                 * 8. Check overall lightest
                 * */

//                bloomFilterFlag = false;
                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteSet v1IntersectionSet = new TByteHashSet(triangleIntersectionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(triangleIntersectionSet);
                TByteSet triangleHDIntersectionSet = new TByteHashSet(triangleIntersectionSet);

                v1IntersectionSet.retainAll(v1HDPartitionSet);

                v2IntersectionSet.retainAll(v2HDPartitionSet);

                triangleHDIntersectionSet.retainAll(v1HDPartitionSet);
                triangleHDIntersectionSet.retainAll(v2HDPartitionSet);

                // 1.1 NO REPLICATION
                if (triangleHDIntersectionSet.size() != 0) {
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, triangleHDIntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        return partitionID;
                    }
                }

                // 1.2 NO REPLICATION
                if (!partitionFound) {
                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(1);
                        return partitionID;
                    }
                }

                // 1.3 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1HDPartitionSet);
                    v2IntersectionSet.retainAll(v2HDPartitionSet);

                    if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                        /* find partition where sum of degree is max amongst all options.
                         * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                         * to get the final partitions
                         * */
                        TByteSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                        unionPartitionSet.addAll(v2IntersectionSet);

                        partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(2);
                            return partitionID;
                        }
                    }
                }

                // 1.4 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(3);
                        return partitionID;
                    }
                }

                // 1.5 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v1IntersectionSet.clear();
                    v1IntersectionSet.addAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, v1IntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(4);
                        return partitionID;
                    }

                }


                // 1.6 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(5);
                        return partitionID;
                    }
                }

                // 1.7 POSSIBLE REPLICATIONs
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(6);
                        return partitionID;
                    }
                }
                // 1.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;

                    incrementHitRate(7);
                    return partitionID;
                }

            } else {
                /* v1 and v2 are present in TMAP on different partitions
                 * 1. Check T(v1) intersect H(v2) intersect H(v1) and Check T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect H(v2) and T(v2) intersect H(v1). Choose partition where HD vertex has higher degree .
                 * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 4. Check T(v1) intersect BF(v2) and T(v2) intersect BF(v1). Send to lightest partition.
                 * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check T(v1) intersect H(v1) and T(v2) intersect H(v2). Choose partition where degree is max.[v2HDMap entry exists]
                 * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
                 * 9. Check first of T(v1) [if previously not found in 6.]
                 * 10. Check union of BF(v1) and BF(v2)
                 * 11. Check overall lightest
                 * */

                bloomFilterFlag = false;
                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteSet v1IntersectionSet = new TByteHashSet(v1TrianglePartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2TrianglePartitionSet);


                v1IntersectionSet.retainAll(v1HDPartitionSet);
                v1IntersectionSet.retainAll(v2HDPartitionSet);

                v2IntersectionSet.retainAll(v1HDPartitionSet);
                v2IntersectionSet.retainAll(v2HDPartitionSet);

                // 2.1 NO REPLICATION
                if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                    /* find partition where sum of degree is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */
                    TByteSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                    unionPartitionSet.addAll(v2IntersectionSet);

                    partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(8);
                        return partitionID;
                    }

                }

                // 2.2 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1HDPartitionSet);
                    /* find partition where degree of HD vertex is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v2Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v1Partition)) {
                            partitionID = v2Partition;
                        } else {
                            partitionID = v1Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(9);
                        return partitionID;
                    }
                }

                // 2.3 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                    /* find partition where load is least amongst both sets of partitions.
                     * */
                    TByteHashSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                    //					unionPartitionSet.addAll(v1IntersectionList);
                    unionPartitionSet.addAll(v2IntersectionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionPartitionSet, MultiThreadedMaster.higherThreshold, v1, v2);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(10);
                        return partitionID;
                    }
                }

                // 2.4 NO REPLICATION
                if (!partitionFound) {
                    TByteSet intersectionSet = new TByteHashSet(v1HDPartitionSet);

                    intersectionSet.retainAll(v2HDPartitionSet);
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(11);
                        return partitionID;
                    }

                }


                // 2.5 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                    v2IntersectionSet.addAll(v2HDPartitionSet);
                    v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(12);
                        return partitionID;
                    }

                }

                // 2.6 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(13);
                        return partitionID;
                    }
                }

                // 2.7 POSSIBLE REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v1HDPartitionSet);

                    v2IntersectionSet.addAll(v2HDPartitionSet);
                    v2IntersectionSet.retainAll(v2TrianglePartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(14);
                        return partitionID;
                    }

                }

                // 2.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(15);
                        return partitionID;
                    }
                }

                // 2.9 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(16);
                        return partitionID;
                    }
                }

                // 2.10 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(17);
                        return partitionID;
                    }
                }

                // 2.11 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;
                    incrementHitRate(18);
                    return partitionID;
                }

            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are triangle vertices with available space";

        } else if (v1TrianglePartitionSet.size() != 0) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v1 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v2TrianglePartitionSet.size() == 0) : "v2 has a list of partitions in TMAP but not returned";

            bloomFilterFlag = false;

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            TByteSet intersectionSet = new TByteHashSet(v1TrianglePartitionSet);

            intersectionSet.retainAll(v1HDPartitionSet);
            intersectionSet.retainAll(v2HDPartitionSet);

            // 3.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    incrementHitRate(19);
                    return partitionID;
                }
            }

            // 3.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(20);
                    return partitionID;
                }
            }

            //swapped 21 and 22
            // 3.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v2ReplicaPartitionSet);
//				partitionID = getLeastLoadedPartitionInSet(intersectionSet, higherThreshold, v1, v2);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(21);
                    return partitionID;
                }

            }


            // 3.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1HDPartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(22);
                    return partitionID;
                }
            }


            // 3.5 NO REPLICATION
            if (!partitionFound) {
                TByteSet v1IntersectionSet = new TByteHashSet(v1HDPartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2HDPartitionSet);

                v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(23);
                    return partitionID;
                }
            }

            // 3.6 NO REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(24);
                    return partitionID;
                }
            }

            // 3.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v1HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {

                    partitionFound = true;
                    incrementHitRate(25);
                    return partitionID;
                }
            }

            // 3.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {

                    incrementHitRate(26);
                    partitionFound = true;
                    return partitionID;
                }

            }


            // 3.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(28);
                    return partitionID;
                }
            }

            // 3.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;

                incrementHitRate(29);
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        } else if (v2TrianglePartitionSet.size() != 0) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v2 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v1TrianglePartitionSet.size() == 0) : "v2 has a list of partitions in TMAP but not returned";

            bloomFilterFlag = false;

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            TByteSet intersectionSet = new TByteHashSet(v2TrianglePartitionSet);

            intersectionSet.retainAll(v1HDPartitionSet);
            intersectionSet.retainAll(v2HDPartitionSet);

            // 4.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(30);
                    return partitionID;
                }
            }

            // 4.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v1HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(31);
                    return partitionID;
                }
            }

            //swapped 33 and 32
            // 4.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v1ReplicaPartitionSet);
//				partitionID = getLeastLoadedPartitionInSet(intersectionSet, higherThreshold, v1, v2);
                if (partitionID != -1) {
                    incrementHitRate(32);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1HDPartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    incrementHitRate(33);
                    partitionFound = true;
                    return partitionID;
                }
            }


            // 4.5 NO REPLICATION
            if (!partitionFound) {
                TByteSet v1IntersectionSet = new TByteHashSet(v1HDPartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2HDPartitionSet);

                v1IntersectionSet.retainAll(v2ReplicaPartitionSet);
                v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(34);
                    return partitionID;
                }
            }

            // 4.6 NO REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(35);
                    return partitionID;
                }
            }

            // 4.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(36);
                    return partitionID;
                }
            }

            // 4.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(37);
                    return partitionID;
                }
            }

            // 4.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {

                    incrementHitRate(39);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                incrementHitRate(40);
                partitionFound = true;
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        }

        if (bloomFilterFlag) {
            /* Neither of the vertices are either in triangle or high degree maps.
             * Thus check in bloom filters if vertices have been seen previously.
             *
             * 1. Check lightest in intersection
             * 2. Check lightest in union - valid even if only one vertex is present
             * 3. Check overall lightest
             * */

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);

            // 9.1 NO REPLICATION
            if (partitionID != -1) {
                incrementHitRate(61);
                partitionFound = true;
                return partitionID;
            }

            // 10.1 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(62);
                    return partitionID;
                }
            }

            // 11.1 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;

                incrementHitRate(63);
                return partitionID;
            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are only possibly seen in BF.";
        }

        assert (partitionID != -1) : "Partition ID is not assigned";
        return partitionID;
    }

    private byte getPartition_TrHDNormDynamic_Set(Long v1, Long v2) {
        byte partitionID = -1;
        boolean highDegFlag = true;
        boolean bloomFilterFlag = true;

        boolean partitionFound = false;

        /* Lists to store partitions for vertices in TMAP */
        TByteHashSet v1TrianglePartitionSet;
        TByteHashSet v2TrianglePartitionSet;

        /* Lists to store partitions for vertices in HDMAP */
        TByteHashSet v1HDPartitionSet;
        TByteHashSet v2HDPartitionSet;

        /* Lists to store partitions for vertices in Bloom Filters */
        TByteHashSet v1ReplicaPartitionSet;
        TByteHashSet v2ReplicaPartitionSet;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        v1TrianglePartitionSet = MultiThreadedMaster.vertexTrianglePartitionMap.get(v1);

        v2TrianglePartitionSet = MultiThreadedMaster.vertexTrianglePartitionMap.get(v2);


        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.containsKey(v1)) {
                v1HDPartitionSet = new TByteHashSet(MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).keySet());//(TIntSet);
        } else {
            v1HDPartitionSet = null;
        }

        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.containsKey(v2)) {
            v2HDPartitionSet = new TByteHashSet(MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).keySet());//(TIntSet);
        } else {
            v2HDPartitionSet = null;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if (v1TrianglePartitionSet != null && v2TrianglePartitionSet != null && !v1TrianglePartitionSet.isEmpty() && !v2TrianglePartitionSet.isEmpty()) {
            /* Both vertices are present in TMAP with atleast 1 partition */
            assert (!v1TrianglePartitionSet.isEmpty() && !v2TrianglePartitionSet.isEmpty()) : "Atleast one of the TMAP entries has no "
                    + "set of partitions";

            highDegFlag = false;
            bloomFilterFlag = false;

            TByteHashSet triangleIntersectionSet = setIntersection(v1TrianglePartitionSet, v2TrianglePartitionSet);

            if (triangleIntersectionSet.size() != 0) {
                /* v1 and v2 on same partition(s) in TMAP
                 * 1. Check T(v1) intersect T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect T(v2) intersect H(v1) and T(v1) intersect T(v2) intersect H(v2). Choose partition which has highest degree vertex.
                 * 3. Check T(v1) intersect H(v2) intersect H(v1) and T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 4. Check lightest in T(v1) intersect T(v2)
                 * 5. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check union of BF(v1) and BF(v2)
                 * 8. Check overall lightest
                 * */

                highDegFlag = false;
                bloomFilterFlag = false;
                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteHashSet triangleHDIntersectionSet = setIntersection(triangleIntersectionSet, v1HDPartitionSet, v2HDPartitionSet);

                // 1.1 NO REPLICATION
                if (triangleHDIntersectionSet.size() != 0) {
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, triangleHDIntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(0);
                        return partitionID;
                    }
                }

                // 1.2 NO REPLICATION
                if (!partitionFound) {
                    TByteHashSet v1IntersectionSet = setIntersection(triangleIntersectionSet, v1HDPartitionSet);
                    TByteHashSet v2IntersectionSet = setIntersection(triangleIntersectionSet, v2HDPartitionSet);
                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(1);
                        return partitionID;
                    }
                }

                // 1.3 NO REPLICATION
                if (!partitionFound) {

                    TByteHashSet v1IntersectionSet = setIntersection(v1TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);


                    TByteHashSet v2IntersectionSet = setIntersection(v2TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);

                    if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                        /* find partition where sum of degree is max amongst all options.
                         * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                         * to get the final partitions
                         * */

//
                        TByteHashSet unionPartitionSet = setUnion(v1IntersectionSet, v2IntersectionSet);

                        partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(2);
                            return partitionID;
                        }
                    }
                }

                // 1.4 NO REPLICATION
                if (!partitionFound) {
				   TByteHashSet intersectionSet = setIntersection(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(3);
                        return partitionID;
                    }
                }

                // 1.5 NO REPLICATION
                if (!partitionFound) {

                    TByteHashSet v1IntersectionSet = setIntersection(v1HDPartitionSet, v2HDPartitionSet);

                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, v1IntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(4);
                        return partitionID;
                    }

                }


                // 1.6 NO REPLICATION
                if (!partitionFound) {
                   TByteHashSet intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(5);
                        return partitionID;
                    }
                }

                // 1.7 POSSIBLE REPLICATIONs
                if (!partitionFound) {
                    TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(6);
                        return partitionID;
                    }
                }
                // 1.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;
                    incrementHitRate(7);
                    return partitionID;
                }

            } else {
                /* v1 and v2 are present in TMAP on different partitions
                 * 1. Check T(v1) intersect H(v2) intersect H(v1) and Check T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect H(v2) and T(v2) intersect H(v1). Choose partition where HD vertex has higher degree .
                 * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 4. Check T(v1) intersect BF(v2) and T(v2) intersect BF(v1). Send to lightest partition.
                 * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check T(v1) intersect H(v1) and T(v2) intersect H(v2). Choose partition where degree is max.[v2HDMap entry exists]
                 * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
                 * 9. Check first of T(v1) [if previously not found in 6.]
                 * 10. Check union of BF(v1) and BF(v2)
                 * 11. Check overall lightest
                 * */

                highDegFlag = false;
                bloomFilterFlag = false;

                TByteHashSet v1IntersectionSet = setIntersection(v1TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);

                TByteHashSet v2IntersectionSet = setIntersection(v2TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);

                // 2.1 NO REPLICATION
                if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                    /* find partition where sum of degree is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */

                    TByteHashSet unionPartitionSet = setUnion(v1IntersectionSet, v2IntersectionSet);

                    partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(8);
                        return partitionID;
                    }

                }

                // 2.2 NO REPLICATION
                if (!partitionFound) {
                       v1IntersectionSet = setIntersection(v1TrianglePartitionSet, v2HDPartitionSet);
                      v2IntersectionSet = setIntersection(v2TrianglePartitionSet, v1HDPartitionSet);

                    /* find partition where degree of HD vertex is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v2Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v1Partition)) {
                            partitionID = v2Partition;
                        } else {
                            partitionID = v1Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(9);
                        return partitionID;
                    }
                }

                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);
                // 2.3 NO REPLICATION
                if (!partitionFound) {

                  v1IntersectionSet = setIntersection(v1TrianglePartitionSet, v2ReplicaPartitionSet);

                    v2IntersectionSet = setIntersection(v2TrianglePartitionSet, v1ReplicaPartitionSet);

                    /* find partition where load is least amongst both sets of partitions.
                     * */
                    TByteHashSet unionPartitionSet = setUnion(v1IntersectionSet, v2IntersectionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionPartitionSet, MultiThreadedMaster.higherThreshold, v1, v2);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(10);
                        return partitionID;
                    }
                }

                // 2.4 NO REPLICATION
                if (!partitionFound) {
                    TByteHashSet intersectionSet = setIntersection(v1HDPartitionSet, v2HDPartitionSet);

                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(11);
                        return partitionID;
                    }

                }


                // 2.5 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                    v2IntersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(12);
                        return partitionID;
                    }

                }

                // 2.6 NO REPLICATION
                if (!partitionFound) {
                    TByteHashSet intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(13);
                        return partitionID;
                    }
                }

                // 2.7 POSSIBLE REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet = setIntersection(v1TrianglePartitionSet, v1HDPartitionSet);

                    v2IntersectionSet = setIntersection(v2HDPartitionSet, v2TrianglePartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(14);
                        return partitionID;
                    }

                }

                // 2.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(15);
                        return partitionID;
                    }
                }

                // 2.9 POSSIBLE REPLICATION
                if (!partitionFound) {
                    TByteHashSet unionSet = setUnion(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(16);
                        return partitionID;
                    }
                }

                // 2.10 POSSIBLE REPLICATION
                if (!partitionFound) {
                   TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                       incrementHitRate(17);
                        return partitionID;
                    }
                }

                // 2.11 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;
                    incrementHitRate(18);
                    return partitionID;
                }

            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are triangle vertices with available space";

        } else if (v1TrianglePartitionSet != null && !v1TrianglePartitionSet.isEmpty()) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v1 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v2TrianglePartitionSet == null || v2TrianglePartitionSet.isEmpty()) : "v2 has a list of partitions in TMAP but not returned";

            highDegFlag = false;
            bloomFilterFlag = false;


            TByteHashSet intersectionSet = setIntersection(v1TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);

            // 3.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    incrementHitRate(19);
                    return partitionID;
                }
            }

            // 3.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1TrianglePartitionSet, v2HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(20);
                    return partitionID;
                }
            }


            v2ReplicaPartitionSet = getVertexBFSet(v2);

            //swapped 21 and 22
            // 3.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1TrianglePartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(21);
                    return partitionID;
                }

            }


            // 3.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1HDPartitionSet, v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(22);
                    return partitionID;
                }
            }

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            // 3.5 NO REPLICATION
            if (!partitionFound) {
                TByteHashSet v1IntersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                TByteHashSet v2IntersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(23);
                    return partitionID;
                }
            }

            // 3.6 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(24);
                    return partitionID;
                }
            }

            // 3.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1TrianglePartitionSet, v1HDPartitionSet);

                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(25);
                    return partitionID;
                }
            }

            // 3.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {
                    incrementHitRate(26);
                    partitionFound = true;
                    return partitionID;
                }

            }

            // 3.9 POSSIBLE REPLICATION
            if ((v1HDPartitionSet == null || v1HDPartitionSet.isEmpty()) && !partitionFound) {
                byte v1FirstTrianglePartition = MultiThreadedMaster.vertexFirstTrianglePartitionMap.get(v1);
                if (MultiThreadedMaster.currentPartitionSizes[v1FirstTrianglePartition].get() < MultiThreadedMaster.higherThreshold) {
                    partitionID = v1FirstTrianglePartition;
                    partitionFound = true;
                    incrementHitRate(27);
                    return partitionID;
                } else {
                    partitionID = -1;
                }
            }

            // 3.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(28);
                    return partitionID;
                }
            }

            // 3.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;
                incrementHitRate(29);
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        } else if (v2TrianglePartitionSet != null && !v2TrianglePartitionSet.isEmpty()) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v2 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v1TrianglePartitionSet == null || v1TrianglePartitionSet.isEmpty()) : "v2 has a list of partitions in TMAP but not returned";


            highDegFlag = false;
            bloomFilterFlag = false;


            TByteHashSet intersectionSet = setIntersection(v2TrianglePartitionSet, v1HDPartitionSet, v2HDPartitionSet);

            // 4.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(30);
                    return partitionID;
                }
            }

            // 4.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v2TrianglePartitionSet, v1HDPartitionSet);

                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(31);
                    return partitionID;
                }
            }

            v1ReplicaPartitionSet = getVertexBFSet(v1);


            // 4.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v2TrianglePartitionSet, v1ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    incrementHitRate(32);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v1HDPartitionSet, v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);

                if (partitionID != -1) {
                    incrementHitRate(33);
                    partitionFound = true;
                    return partitionID;
                }
            }

            v2ReplicaPartitionSet = getVertexBFSet(v2);
            // 4.5 NO REPLICATION
            if (!partitionFound) {
                TByteHashSet v1IntersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                TByteHashSet v2IntersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(34);
                    return partitionID;
                }
            }

            // 4.6 NO REPLICATION
            if (!partitionFound) {
               intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(35);
                    return partitionID;
                }
            }

            // 4.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet = setIntersection(v2TrianglePartitionSet, v2HDPartitionSet);

                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(36);
                    return partitionID;
                }
            }

            // 4.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(37);
                    return partitionID;
                }
            }

            // 4.9 POSSIBLE REPLICATION
            if ((v2HDPartitionSet == null || v2HDPartitionSet.isEmpty()) && !partitionFound) {
                byte v2FirstTrianglePartition = MultiThreadedMaster.vertexFirstTrianglePartitionMap.get(v2);
                if (MultiThreadedMaster.currentPartitionSizes[v2FirstTrianglePartition].get() < MultiThreadedMaster.higherThreshold) {
                    partitionID = v2FirstTrianglePartition;
                    partitionFound = true;
                    incrementHitRate(38);
                    return partitionID;
                } else {
                    partitionID = -1;
                }
            }

            // 4.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    incrementHitRate(39);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                incrementHitRate(40);
                partitionFound = true;
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        }
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if (highDegFlag) {
            /*
             * Neither of the vertices are part of any triangle
             * */
            if (v1HDPartitionSet != null && v2HDPartitionSet != null && !v1HDPartitionSet.isEmpty() && !v2HDPartitionSet.isEmpty()) {
                bloomFilterFlag = false;

                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteHashSet intersectionSet = setIntersection(v1HDPartitionSet, v2HDPartitionSet);

                if (intersectionSet.size() != 0) {
                    /*
                     * Get partition where sum of degree is maximised
                     * 1. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                     * 2. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
                     * 3. Check lightest in intersection of BF(v1) and BF(v2)
                     * 4. Check union of BF(v1) and BF(v2)
                     * 5. Check overall lightest
                     * */
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);

                    // 5.1 NO REPLICATION
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(41);
                        return partitionID;
                    }

                    // 5.2 NO REPLICATION
                    if (!partitionFound) {
                         TByteHashSet v1IntersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                         TByteHashSet v2IntersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                        /* call partition with max degree between the two lists
                         * */
                        partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(42);
                            return partitionID;
                        }
                    }

                    // 5.3 NO REPLICATION
                    if (!partitionFound) {
                        intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                        partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(43);
                            return partitionID;
                        }

                    }

                    // 5.4 POSSIBLE REPLICATION
                    if (!partitionFound) {
                       TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                        partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(44);
                            return partitionID;
                        }
                    }

                    // 5.5 POSSIBLE REPLICATION
                    if (!partitionFound) {
                        partitionID = getLightestPartition();
                        partitionFound = true;
                        incrementHitRate(45);
                        return partitionID;
                    }

                } else {
                    bloomFilterFlag = false;

                    /*
                     * 1. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
                     * 2. Check lightest in intersection of BF(v1) and BF(v2)
                     * 3. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
                     * 4. Check union of BF(v1) and BF(v2)
                     * 5. Check overall lightest
                     * */

                    TByteHashSet v1IntersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                    TByteHashSet v2IntersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                    /* call partition with max degree between the two lists
                     * */
                    // 6.1 NO REPLICATION
                    partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(46);
                        return partitionID;
                    }

                    // 6.2 NO REPLICATION
                    if (!partitionFound) {
                        intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                        partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(47);
                            return partitionID;
                        }

                    }

                    // 6.3 POSSIBLE REPLICATION
                    if (!partitionFound) {
                        partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(48);
                            return partitionID;
                        }
                    }

                    // 6.4 POSSIBLE REPLICATION
                    if (!partitionFound) {
                        TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                        partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(49);
                            return partitionID;
                        }
                    }

                    // 6.5 POSSIBLE REPLICATION
                    if (!partitionFound) {
                        partitionID = getLightestPartition();
                        partitionFound = true;
                        incrementHitRate(50);
                        return partitionID;
                    }
                    assert (partitionID != -1) : "Partition ID is not assigned";
                }


            } else if (v1HDPartitionSet != null && !v1HDPartitionSet.isEmpty()) {

                /*
                 * 1. Check intersection of H(v1) with BF(v2)
                 * 2. Check intersection of BF(v1) and BF(v2)
                 * 3. Check Partition which hosts max degree HD replica of v1
                 * 4. Check union of BF(v1) and BF(v2)
                 * 5. Check overall lightest
                 * */

                bloomFilterFlag = false;
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteHashSet intersectionSet = setIntersection(v1HDPartitionSet, v2ReplicaPartitionSet);

                // 7.1 NO REPLICATION
                if (intersectionSet.size() != 0) {
                    /* call partition with max degree
                     * */
                    partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(51);
                        return partitionID;
                    }

                }

                v1ReplicaPartitionSet = getVertexBFSet(v1);
                // 7.2 NO REPLICATION
                if (!partitionFound) {
                    intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(52);
                        return partitionID;
                    }

                }

                // 7.3 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1HDPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(53);
                        return partitionID;
                    }
                }

                // 7.4 POSSIBLE REPLICATION
                if (!partitionFound) {
                    TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(54);
                        return partitionID;
                    }
                }

                // 7.5 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;
                    incrementHitRate(55);
                    return partitionID;
                }

                assert (partitionID != -1) : "Partition ID is not assigned when v1 is HD vertices, v2 has lesser degree";

            } else if (v2HDPartitionSet != null && !v2HDPartitionSet.isEmpty()) {

                /*
                 * 1. Check intersection of H(v2) with BF(v1)
                 * 2. Check intersection of BF(v1) and BF(v2)
                 * 3. Check Partition which hosts max degree HD replica of v2
                 * 4. Check union of BF(v1) and BF(v2)
                 * 5. Check overall lightest
                 * */

                bloomFilterFlag = false;
                v1ReplicaPartitionSet = getVertexBFSet(v1);

                TByteHashSet intersectionSet = setIntersection(v2HDPartitionSet, v1ReplicaPartitionSet);

                // 8.1 NO REPLICATION
                if (intersectionSet.size() != 0) {
                    /* call partition with max degree
                     * */
                    partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                    if (partitionID != -1) {
                        incrementHitRate(56);
                        return partitionID;
                    }
                }

                v2ReplicaPartitionSet = getVertexBFSet(v2);
                // 8.2 NO REPLICATION
                if (!partitionFound) {
                    intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(57);
                        return partitionID;
                    }
                }

                // 8.3 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2HDPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(58);
                        return partitionID;
                    }
                }

                // 8.4 POSSIBLE REPLICATION
                if (!partitionFound) {
                    TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(59);
                        return partitionID;
                    }
                }

                // 8.5 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;

                    incrementHitRate(60);
                    return partitionID;
                }

                assert (partitionID != -1) : "Partition ID is not assigned when v1 is HD vertices, v2 has lesser degree";

            }
        }

        if(bloomFilterFlag) {
            /* Neither of the vertices are either in triangle or high degree maps.
             * Thus check in bloom filters if vertices have been seen previously.
             *
             * 1. Check lightest in intersection
             * 2. Check lightest in union - valid even if only one vertex is present
             * 3. Check overall lightest
             * */

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            TByteHashSet intersectionSet = setIntersection(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
            partitionID = getLeastLoadedPartitionInSet(intersectionSet, MultiThreadedMaster.higherThreshold, v1, v2);

            // 9.1 NO REPLICATION
            if (partitionID != -1) {
                incrementHitRate(61);
                partitionFound = true;
                return partitionID;
            }

            // 10.1 POSSIBLE REPLICATION
            if (!partitionFound) {

                TByteHashSet unionSet = setUnion(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                partitionID = getLeastLoadedPartitionInSet(unionSet, MultiThreadedMaster.higherThreshold, v1, v2);

                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(62);
                    return partitionID;
                }
            }

            // 11.1 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;
                incrementHitRate(63);
                return partitionID;
            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are only possibly seen in BF.";
        }

        assert (partitionID != -1) : "Partition ID is not assigned";
        return partitionID;
    }

    private byte getPartition_TrNormDynamic_Set(long v1, long v2) {
        MultiThreadedMaster.decisionHandlerEdgeCount.incrementAndGet();

        byte partitionID = -1;
        boolean bloomFilterFlag = true;

        boolean partitionFound = false;

        /* Lists to store partitions for vertices in TMAP */
        TByteSet v1TrianglePartitionSet;
        TByteSet v2TrianglePartitionSet;

        /* Lists to store partitions for vertices in HDMAP but never used */
        TByteSet v1HDPartitionSet = new TByteHashSet();
        TByteSet v2HDPartitionSet = new TByteHashSet();

        /* Lists to store partitions for vertices in Bloom Filters */
        TByteSet v1ReplicaPartitionSet;
        TByteSet v2ReplicaPartitionSet;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


        if (MultiThreadedMaster.vertexTrianglePartitionMap.containsKey(v1)) {
            v1TrianglePartitionSet = new TByteHashSet(MultiThreadedMaster.vertexTrianglePartitionMap.get(v1));
        } else {
            v1TrianglePartitionSet = new TByteHashSet();
        }

        if (MultiThreadedMaster.vertexTrianglePartitionMap.containsKey(v2)) {
            v2TrianglePartitionSet = new TByteHashSet(MultiThreadedMaster.vertexTrianglePartitionMap.get(v2));
        } else {
            v2TrianglePartitionSet = new TByteHashSet();
        }


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if ((v1TrianglePartitionSet.size() != 0) && (v2TrianglePartitionSet.size() != 0)) {
            /* Both vertices are present in TMAP with atleast 1 partition */
            assert (!v1TrianglePartitionSet.isEmpty() && !v2TrianglePartitionSet.isEmpty()) : "Atleast one of the TMAP entries has no "
                    + "list of partitions";

            bloomFilterFlag = false;

            TByteSet triangleIntersectionSet = new TByteHashSet(v1TrianglePartitionSet);

            triangleIntersectionSet.retainAll(v2TrianglePartitionSet);

            if (triangleIntersectionSet.size() != 0) {
                /* v1 and v2 on same partition(s) in TMAP
                 * 1. Check T(v1) intersect T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect T(v2) intersect H(v1) and T(v1) intersect T(v2) intersect H(v2). Choose partition which has highest degree vertex.
                 * 3. Check T(v1) intersect H(v2) intersect H(v1) and T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 4. Check lightest in T(v1) intersect T(v2)
                 * 5. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check union of BF(v1) and BF(v2)
                 * 8. Check overall lightest
                 * */

                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteSet v1IntersectionSet = new TByteHashSet(triangleIntersectionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(triangleIntersectionSet);
                TByteSet triangleHDIntersectionSet = new TByteHashSet(triangleIntersectionSet);

                v1IntersectionSet.retainAll(v1HDPartitionSet);

                v2IntersectionSet.retainAll(v2HDPartitionSet);

                triangleHDIntersectionSet.retainAll(v1HDPartitionSet);
                triangleHDIntersectionSet.retainAll(v2HDPartitionSet);

                // 1.1 NO REPLICATION
                if (triangleHDIntersectionSet.size() != 0) {
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, triangleHDIntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        return partitionID;
                    }
                }

                // 1.2 NO REPLICATION
                if (!partitionFound) {
                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(1);
                        return partitionID;
                    }
                }

                // 1.3 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1HDPartitionSet);
                    v2IntersectionSet.retainAll(v2HDPartitionSet);

                    if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                        /* find partition where sum of degree is max amongst all options.
                         * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                         * to get the final partitions
                         * */
                        TByteSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                        unionPartitionSet.addAll(v2IntersectionSet);

                        partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                        if (partitionID != -1) {
                            partitionFound = true;
                            incrementHitRate(2);
                            return partitionID;
                        }
                    }
                }

                // 1.4 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(3);
                        return partitionID;
                    }
                }

                // 1.5 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v1IntersectionSet.clear();
                    v1IntersectionSet.addAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, v1IntersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(4);
                        return partitionID;
                    }

                }


                // 1.6 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(5);
                        return partitionID;
                    }
                }

                // 1.7 POSSIBLE REPLICATIONs
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(6);
                        return partitionID;
                    }
                }
                // 1.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;

                    incrementHitRate(7);
                    return partitionID;
                }

            } else {
                /* v1 and v2 are present in TMAP on different partitions
                 * 1. Check T(v1) intersect H(v2) intersect H(v1) and Check T(v2) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
                 * 2. Check T(v1) intersect H(v2) and T(v2) intersect H(v1). Choose partition where HD vertex has higher degree .
                 * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
                 * 4. Check T(v1) intersect BF(v2) and T(v2) intersect BF(v1). Send to lightest partition.
                 * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
                 * 6. Check lightest in intersection of BF(v1) and BF(v2)
                 * 7. Check T(v1) intersect H(v1) and T(v2) intersect H(v2). Choose partition where degree is max.[v2HDMap entry exists]
                 * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
                 * 9. Check first of T(v1) [if previously not found in 6.]
                 * 10. Check union of BF(v1) and BF(v2)
                 * 11. Check overall lightest
                 * */

                bloomFilterFlag = false;
                v1ReplicaPartitionSet = getVertexBFSet(v1);
                v2ReplicaPartitionSet = getVertexBFSet(v2);

                TByteSet v1IntersectionSet = new TByteHashSet(v1TrianglePartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2TrianglePartitionSet);


                v1IntersectionSet.retainAll(v1HDPartitionSet);
                v1IntersectionSet.retainAll(v2HDPartitionSet);

                v2IntersectionSet.retainAll(v1HDPartitionSet);
                v2IntersectionSet.retainAll(v2HDPartitionSet);

                // 2.1 NO REPLICATION
                if (!v1IntersectionSet.isEmpty() || !v2IntersectionSet.isEmpty()) {
                    /* find partition where sum of degree is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */
                    TByteSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                    unionPartitionSet.addAll(v2IntersectionSet);

                    partitionID = getMaxDegreeSumPartitionFromSet(v1, v2, unionPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(8);
                        return partitionID;
                    }

                }

                // 2.2 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v2HDPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1HDPartitionSet);
                    /* find partition where degree of HD vertex is max amongst all options.
                     * get max degree partition of v1 and v2 and then compare the degree in the two partitions
                     * to get the final partitions
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v2Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v1Partition)) {
                            partitionID = v2Partition;
                        } else {
                            partitionID = v1Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(9);
                        return partitionID;
                    }
                }

                // 2.3 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                    v2IntersectionSet.addAll(v2TrianglePartitionSet);
                    v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                    /* find partition where load is least amongst both sets of partitions.
                     * */
                    TByteHashSet unionPartitionSet = new TByteHashSet(v1IntersectionSet);
                    unionPartitionSet.addAll(v2IntersectionSet);
                    partitionID = getLeastLoadedPartitionInSet(unionPartitionSet, MultiThreadedMaster.higherThreshold, v1, v2);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(10);
                        return partitionID;
                    }
                }

                // 2.4 NO REPLICATION
                if (!partitionFound) {
                    TByteSet intersectionSet = new TByteHashSet(v1HDPartitionSet);

                    intersectionSet.retainAll(v2HDPartitionSet);
                    partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(11);
                        return partitionID;
                    }

                }


                // 2.5 NO REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1HDPartitionSet);
                    v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                    v2IntersectionSet.addAll(v2HDPartitionSet);
                    v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(12);
                        return partitionID;
                    }

                }

                // 2.6 NO REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(13);
                        return partitionID;
                    }
                }

                // 2.7 POSSIBLE REPLICATION
                if (!partitionFound) {
                    v1IntersectionSet.clear();
                    v2IntersectionSet.clear();

                    v1IntersectionSet.addAll(v1TrianglePartitionSet);
                    v1IntersectionSet.retainAll(v1HDPartitionSet);

                    v2IntersectionSet.addAll(v2HDPartitionSet);
                    v2IntersectionSet.retainAll(v2TrianglePartitionSet);

                    /* find partition where higher degree vertex is present.
                     * */

                    byte v1Partition = getPartitionOfMaxDegreeVertexFromSet(v1, v2, v1IntersectionSet);
                    byte v2Partition = getPartitionOfMaxDegreeVertexFromSet(v2, v1, v2IntersectionSet);

                    if (v1Partition != -1 && v2Partition != -1) {
                        if (MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v1).get(v1Partition) >= MultiThreadedMaster.vertexHDPartitionDegreeMap.get(v2).get(v2Partition)) {
                            partitionID = v1Partition;
                        } else {
                            partitionID = v2Partition;
                        }
                    } else if (v1Partition != -1) {
                        partitionID = v1Partition;
                    } else if (v2Partition != -1) {
                        partitionID = v2Partition;
                    } else {
                        partitionID = -1;
                    }

                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(14);
                        return partitionID;
                    }

                }

                // 2.8 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(15);
                        return partitionID;
                    }
                }

                // 2.9 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1TrianglePartitionSet, v2TrianglePartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;
                        incrementHitRate(16);
                        return partitionID;
                    }
                }

                // 2.10 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                    if (partitionID != -1) {
                        partitionFound = true;

                        incrementHitRate(17);
                        return partitionID;
                    }
                }

                // 2.11 POSSIBLE REPLICATION
                if (!partitionFound) {
                    partitionID = getLightestPartition();
                    partitionFound = true;

                    incrementHitRate(18);
                    return partitionID;
                }

            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are triangle vertices with available space";

        } else if (v1TrianglePartitionSet.size() != 0) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v1 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v2TrianglePartitionSet.size() == 0) : "v2 has a list of partitions in TMAP but not returned";

            bloomFilterFlag = false;

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            TByteSet intersectionSet = new TByteHashSet(v1TrianglePartitionSet);

            intersectionSet.retainAll(v1HDPartitionSet);
            intersectionSet.retainAll(v2HDPartitionSet);

            // 3.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    incrementHitRate(19);
                    return partitionID;
                }
            }

            // 3.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(20);
                    return partitionID;
                }
            }

            //swapped 21 and 22
            // 3.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(21);
                    return partitionID;
                }

            }


            // 3.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1HDPartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(22);
                    return partitionID;
                }
            }


            // 3.5 NO REPLICATION
            if (!partitionFound) {
                TByteSet v1IntersectionSet = new TByteHashSet(v1HDPartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2HDPartitionSet);

                v1IntersectionSet.retainAll(v2ReplicaPartitionSet);

                v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(23);
                    return partitionID;
                }
            }

            // 3.6 NO REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(24);
                    return partitionID;
                }
            }

            // 3.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1TrianglePartitionSet);
                intersectionSet.retainAll(v1HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(25);
                    return partitionID;
                }
            }

            // 3.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {

                    incrementHitRate(26);
                    partitionFound = true;
                    return partitionID;
                }

            }

            // 3.9 POSSIBLE REPLICATION
            if (v1HDPartitionSet.size() == 0 && !partitionFound) {
                byte v1FirstTrianglePartition = MultiThreadedMaster.vertexFirstTrianglePartitionMap.get(v1);
                if (MultiThreadedMaster.currentPartitionSizes[v1FirstTrianglePartition].get() < MultiThreadedMaster.higherThreshold) {
                    partitionID = v1FirstTrianglePartition;
                    partitionFound = true;

                    incrementHitRate(27);
                    return partitionID;
                } else {
                    partitionID = -1;
                }
            }

            // 3.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(28);
                    return partitionID;
                }
            }

            // 3.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;

                incrementHitRate(29);
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        } else if (v2TrianglePartitionSet.size() != 0) {
            /* v1 is present in TMAP with atleast 1 partition
             * 1. Check T(v1) intersect H(v2) intersect H(v1). Choose partition where sum of degree is max.
             * 2. Check T(v1) intersect H(v2). Choose partition where degree of v2 is max.
             * 3. Check H(v1) intersect H(v2). Choose partition where sum of degree is max.
             * 4. Check T(v1) intersect BF(v2). Send to lightest partition.
             * 5. Check H(v1) intersect BF(v2) and H(v2) intersect BF(v1). Choose partition where degree is max.
             * 6. Check lightest in intersection of BF(v1) and BF(v2)
             * 7. Check T(v1) intersect H(v1). Choose partition where degree of v2 is max.
             * 8. Check partition from two lists- H(v1) and H(v2), where highest degree replica is present
             * 9. Check first of T(v1) [if previously not found in 6.]
             * 10. Check union of BF(v1) and BF(v2)
             * 11. Check overall lightest
             * */

            assert (v1TrianglePartitionSet.size() == 0) : "v2 has a list of partitions in TMAP but not returned";

            bloomFilterFlag = false;

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            TByteSet intersectionSet = new TByteHashSet(v2TrianglePartitionSet);

            intersectionSet.retainAll(v1HDPartitionSet);
            intersectionSet.retainAll(v2HDPartitionSet);

            // 4.1 NO REPLICATION
            if (intersectionSet.size() != 0) {
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(30);
                    return partitionID;
                }
            }

            // 4.2 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v1HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v1, v2, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(31);
                    return partitionID;
                }
            }

            //swapped 33 and 32
            // 4.3 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v1ReplicaPartitionSet);
                if (partitionID != -1) {
                    incrementHitRate(32);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.4 NO REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v1HDPartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getMaxDegreeSumPartitionFromSet(v1,v2, intersectionSet);
                if (partitionID != -1) {
                    incrementHitRate(33);
                    partitionFound = true;
                    return partitionID;
                }
            }


            // 4.5 NO REPLICATION
            if (!partitionFound) {
                TByteSet v1IntersectionSet = new TByteHashSet(v1HDPartitionSet);
                TByteSet v2IntersectionSet = new TByteHashSet(v2HDPartitionSet);

                v1IntersectionSet.retainAll(v2ReplicaPartitionSet);
                v2IntersectionSet.retainAll(v1ReplicaPartitionSet);

                /* call partition with max degree between the two lists
                 * */
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1IntersectionSet, v2IntersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(34);
                    return partitionID;
                }
            }

            // 4.6 NO REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(35);
                    return partitionID;
                }
            }

            // 4.7 POSSIBLE REPLICATION
            if (!partitionFound) {
                intersectionSet.clear();
                intersectionSet.addAll(v2TrianglePartitionSet);
                intersectionSet.retainAll(v2HDPartitionSet);
                partitionID = getPartitionOfMaxDegreeVertexFromSet(v2, v1, intersectionSet);
                if (partitionID != -1) {
                    partitionFound = true;
                    incrementHitRate(36);
                    return partitionID;
                }
            }

            // 4.8 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfMaxDegreeFromTwoSets(v1, v2, v1HDPartitionSet, v2HDPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(37);
                    return partitionID;
                }
            }

            // 4.9 POSSIBLE REPLICATION
            if (v2HDPartitionSet.size() == 0 && !partitionFound) {
                byte v2FirstTrianglePartition = MultiThreadedMaster.vertexFirstTrianglePartitionMap.get(v2);
                if (MultiThreadedMaster.currentPartitionSizes[v2FirstTrianglePartition].get() < MultiThreadedMaster.higherThreshold) {
                    partitionID = v2FirstTrianglePartition;
                    partitionFound = true;

                    incrementHitRate(38);
                    return partitionID;
                } else {
                    partitionID = -1;
                }
            }

            // 4.10 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {

                    incrementHitRate(39);
                    partitionFound = true;
                    return partitionID;
                }
            }

            // 4.11 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();

                incrementHitRate(40);
                partitionFound = true;
                return partitionID;
            }

            assert (partitionID != -1): "No Partition ID has been assigned when only vertex v1 has triangleVertexMap entry and v2(if seen)'s "
                    + "partition doesn't have space";

        }

        if (bloomFilterFlag) {
            /* Neither of the vertices are either in triangle or high degree maps.
             * Thus check in bloom filters if vertices have been seen previously.
             *
             * 1. Check lightest in intersection
             * 2. Check lightest in union - valid even if only one vertex is present
             * 3. Check overall lightest
             * */

            v1ReplicaPartitionSet = getVertexBFSet(v1);
            v2ReplicaPartitionSet = getVertexBFSet(v2);

            partitionID = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);

            // 9.1 NO REPLICATION
            if (partitionID != -1) {
                incrementHitRate(61);
                partitionFound = true;
                return partitionID;
            }

            // 10.1 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
                if (partitionID != -1) {
                    partitionFound = true;

                    incrementHitRate(62);
                    return partitionID;
                }
            }

            // 11.1 POSSIBLE REPLICATION
            if (!partitionFound) {
                partitionID = getLightestPartition();
                partitionFound = true;

                incrementHitRate(63);
                return partitionID;
            }

            assert (partitionID != -1) : "Partition ID is not assigned when v1 and v2 are only possibly seen in BF.";
        }

        assert (partitionID != -1) : "Partition ID is not assigned";
        return partitionID;
    }

    private int getPartition_NormDynamic(long v1, long v2) {
        assert ((v1 != -1L) && (v2 != -1L) && (v1 != v2)) : "Vertices read from Queue are INVALID";

        TByteSet v1ReplicaPartitionSet = getVertexBFSet(v1);
        TByteSet v2ReplicaPartitionSet = getVertexBFSet(v2);

        int partitionId = getPartitionOfIntersectionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
        if (partitionId != -1) {
            incrementHitRate(67);
        }

        if (partitionId == -1) {
            partitionId = getPartitionOfUnionSets(v1ReplicaPartitionSet, v2ReplicaPartitionSet);
            if (partitionId != -1) {
                incrementHitRate(68);
            }
        }


        if (partitionId == -1) {
            partitionId = getLightestPartition();
            if (partitionId != -1) {
                incrementHitRate(69);
            }
        }


        assert (partitionId != -1) : "Partition ID is not assigned when v1 and v2 are only possibly seen in BF.";
        return partitionId;
    }

    private int setPartition(long v1, long v2) {
        int partitionID = -1;
        byte green = 0;
        byte red1 = 1;
        byte red2 = 2;

        switch (MultiThreadedMaster.heuristic) {
            case B:
                partitionID = getPartition_NormDynamic(v1, v2);
                break;

            case BT:
                partitionID = getPartition_TrNormDynamic_Set(v1, v2);
                break;

            case BTH:
                partitionID = getPartition_TrHDNormDynamic_Set(v1, v2);
                break;

            case BH:
                //
                partitionID = getPartition_HDNormDynamic_Set(v1, v2);
                break;
            default:
                LOGGER.info("FATAL ERROR!! Technique doesn't exist!");
                System.exit(0);
                break;
        }

        return partitionID;
    }

    public boolean checkIfSyncIsRequiredAndAcquireLock(long syncEdgeCount) {
        boolean isResponsibleForSync = false;

        if (MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BT) {
            if (syncEdgeCount == MultiThreadedMaster.intervalCounter.get() * MultiThreadedMaster.syncInterval) {
                if (MultiThreadedMaster.updateRequired.compareAndSet(false, true)) {
                    int syncCounter = MultiThreadedMaster.intervalCounter.getAndIncrement();
                    LOGGER.info("---------- MASTER.SYNC CONDITION MET - by" + Thread.currentThread().getId() + " -------- ");
                    LOGGER.info("MASTER. SYNC GLOBAL INTERVAL COUNTER - " + (MultiThreadedMaster.syncCounter.get()+1) +
                            ",MASTER.SYNC EgitDGE INTERVAL COUNTER - " + syncCounter +
                            ",MASTER.SYNC EDGE COUNT - " + syncEdgeCount +
                            ",MASTER.CONDITION COUNT - " + MultiThreadedMaster.syncInterval * MultiThreadedMaster.intervalCounter.get());
                    isResponsibleForSync = true;
                }
            }
        } else if (MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTH || MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTHE) {
            boolean syncEdgeCondition = (syncEdgeCount == MultiThreadedMaster.intervalCounter.get() * MultiThreadedMaster.syncInterval);
            // update only periodically. Since update only by one thread within the period, no need for atomic
            boolean syncDegreeCondition = (syncEdgeCount % AVG_DEGREE_UPDATE_COUNT == 0) ? (newAvgDegree - globalAvgDegree >= 1) :  false; 

            if (syncEdgeCondition || syncDegreeCondition) {
                if (MultiThreadedMaster.updateRequired.compareAndSet(false, true)) {

                    // segregate sync points into degree vs edge
                    if (syncEdgeCondition) {
                        LOGGER.info("---------- MASTER.SYNC CONDITION MET (Edge Sync) - by" + Thread.currentThread().getId() + " -------- ");
                        LOGGER.info("MASTER.SYNC GLOBAL SYNC COUNTER - " + (MultiThreadedMaster.syncCounter.get()+1) +
                                ",MASTER.SYNC EDGE INTERVAL COUNTER - " + MultiThreadedMaster.intervalCounter +
                                ",MASTER.SYNC EDGE COUNT - " + syncEdgeCount +
                                ",MASTER.CONDITION COUNT - " + MultiThreadedMaster.syncInterval * MultiThreadedMaster.intervalCounter.get());
                        MultiThreadedMaster.sync_points_edge_count.add(MultiThreadedMaster.syncCounter.get() + 1);
                        MultiThreadedMaster.intervalCounter.getAndIncrement();
                    }

                    if (syncDegreeCondition) {
                        LOGGER.info("---------- MASTER.SYNC CONDITION MET (Degree Sync) - by" + Thread.currentThread().getId() + " -------- ");
                        LOGGER.info("MASTER.SYNC GLOBAL SYNC COUNTER - " + (MultiThreadedMaster.syncCounter.get()+1) +
                                ",MASTER.SYNC EDGE COUNT - " + syncEdgeCount);
                        MultiThreadedMaster.sync_points_degree_change.add(MultiThreadedMaster.syncCounter.get()+1);
                        globalAvgDegree = newAvgDegree;
                    }

                    isResponsibleForSync = true;
                }
            }
        } else if ( MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BH) {
            // update only periodically. Since update only by one thread within the period, no need for atomic
            boolean syncDegreeCondition = (syncEdgeCount % AVG_DEGREE_UPDATE_COUNT == 0) ? (newAvgDegree - globalAvgDegree >= 1) :  false;

            if (syncDegreeCondition) {
                if (MultiThreadedMaster.updateRequired.compareAndSet(false, true)) {
                        LOGGER.info("---------- MASTER.SYNC CONDITION MET (Degree Sync) - by" + Thread.currentThread().getId() + " -------- ");
                        LOGGER.info("MASTER.SYNC GLOBAL SYNC COUNTER - " + (MultiThreadedMaster.syncCounter.get()+1) +
                                ",MASTER.SYNC EDGE COUNT - " + syncEdgeCount);
                        MultiThreadedMaster.sync_points_degree_change.add(MultiThreadedMaster.syncCounter.get()+1);
                        globalAvgDegree = newAvgDegree;
                    isResponsibleForSync = true;
                }
            }
        }


        return isResponsibleForSync;
    }

      public int calculateAvgDegree(long source, long sink, boolean getEstimateCount) {
        MultiThreadedMaster.vertexEstimator.put(source);
        MultiThreadedMaster.vertexEstimator.put(sink);
        return getEstimateCount ? (int)MultiThreadedMaster.vertexEstimator.approximateElementCount() : 0;
    }

    public void processEdge() {
        while (true) {
            long start = System.currentTimeMillis();
            Edge edge = IOQueue.poll();
            long end = System.currentTimeMillis();
            MultiThreadedMaster.compute_totalIOQueueReadTime.addAndGet((end-start));

            if (edge != null) {
                long decisionStartTime = System.currentTimeMillis();
                int partitionId = setPartition(edge.getSource(), edge.getSink());
                long decisionEndTime = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalDecisionTime.addAndGet((decisionEndTime-decisionStartTime));

                long currentEdgeCount = MultiThreadedMaster.processedEdgeCount.incrementAndGet();
                IOSharedResources.consumedCount.incrementAndGet();

                long avgDegreeCalStart = System.currentTimeMillis();
                if (MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTH || MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTHE || MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BH) {
                    if(currentEdgeCount % AVG_DEGREE_UPDATE_COUNT == 0) { // update only periodically
                        int estimatedVertexCount = calculateAvgDegree(edge.getSource(), edge.getSink(), true);
                        newAvgDegree = 2*currentEdgeCount/(float)estimatedVertexCount;
                    } else {
                        calculateAvgDegree(edge.getSource(), edge.getSink(), false);
                    }
                }
                long avgDegreeCalEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalAvgDegreeCalTime.addAndGet((avgDegreeCalEnd-avgDegreeCalStart));
                /*
                 * Update the Bloom Filter in critical section and add to the transport queue
                 * */

                BloomFilter<Long> partBloomFilter = MultiThreadedMaster.bloomFilterArray[partitionId];
                long sourceId = edge.getSource();
                long sinkId = edge.getSink();

                long updateBloomFilterStart = System.currentTimeMillis();
                partBloomFilter.put(sourceId);
                partBloomFilter.put(sinkId);
                long updateBloomFilterEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalBloomFilterUpdateTime.addAndGet((updateBloomFilterEnd-updateBloomFilterStart));

                long partitionSizeUpdateStart = System.currentTimeMillis();
                MultiThreadedMaster.currentPartitionSizes[partitionId].incrementAndGet();
                long partitionSizeUpdateEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalPartitionSizeUpdateTime.addAndGet((partitionSizeUpdateEnd-partitionSizeUpdateStart));

                long updateThresholdStart = System.currentTimeMillis();
                MultiThreadedMaster.updateThreshold();
                long updateThresholdEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalUpdateThresholdTime.addAndGet((updateThresholdEnd-updateThresholdStart));


                if (currentEdgeCount % 10000 == 0) {
                    MultiThreadedMaster.batch_timestamps.add(System.currentTimeMillis());
                    MultiThreadedMaster.batch_read_edge_count.add(IOSharedResources.readCount.get());
                    MultiThreadedMaster.batch_processed_edge_count.add(currentEdgeCount);
                    MultiThreadedMaster.batch_transported_edge_count.add(MultiThreadedMaster.transporterSentCount.get());
                }

                /* Queue Put */
                long queuePutStartTime = System.currentTimeMillis();
                transportQueues[partitionId].offer(edge);
                long queuePutEndTime = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalQueuePutTime.addAndGet((queuePutEndTime-queuePutStartTime));

                long addLastEdgeStart = System.currentTimeMillis();
                MultiThreadedMaster.partitionLastProcessedEdge.set(partitionId, edge);
                long addLastEdgeEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalLastEdgeAddTime.addAndGet((addLastEdgeEnd-addLastEdgeStart));

                if (currentEdgeCount % (MultiThreadedMaster.SNAPSHOT_EDGE_COUNT) == 0) {
                        String snapshot = Arrays.toString(Arrays.stream(MultiThreadedMaster.hitRate).toArray());
                        MultiThreadedMaster.histogramSnapShots.add(snapshot);
                        MultiThreadedMaster.SNAPSHOT_EDGE_COUNT = MultiThreadedMaster.SNAPSHOT_EDGE_COUNT*10;
                }

                long syncTimeStart = System.currentTimeMillis();
                if (MultiThreadedMaster.heuristic != MultiThreadedHeuristic.B) {
                    long checkConditionStart = System.currentTimeMillis();
                    boolean isRequiredAndAcquired = checkIfSyncIsRequiredAndAcquireLock(currentEdgeCount);
                    long checkConditionEnd = System.currentTimeMillis();
                    MultiThreadedMaster.compute_totalCheckIfSyncReachedTime.addAndGet((checkConditionEnd-checkConditionStart));
                    if (isRequiredAndAcquired) {
                        try {

                                // if context switching time > propagating of updateRequired
                                long waitForComputeStart = System.currentTimeMillis();
                                synchronized (MultiThreadedMaster.syncThreadLockPartition) {
                                    while(MultiThreadedMaster.ACTIVE_THREAD_COUNT != 1) {
                                        MultiThreadedMaster.syncThreadLockPartition.wait();
                                    }
                                }
                                long waitForComputeEnd = System.currentTimeMillis();
// -----------------------------------   COMPUTE BARRIER IS ON! HERE -------------------------------------------------------/
                                MultiThreadedMaster.per_sync_wait_for_compute.add((waitForComputeEnd-waitForComputeStart));
                                MultiThreadedMaster.compute_totalWaitForComputeToFinish.addAndGet((waitForComputeEnd-waitForComputeStart));

                                // -------- Creates a copy of last processed edges at the time of sync
                                Edge[] lastProcessedEdges = new Edge[MultiThreadedMaster.numPartitions];
                                for (int pId = 0; pId < MultiThreadedMaster.numPartitions; pId++) {
                                    Edge boundaryEdge = MultiThreadedMaster.partitionLastProcessedEdge.get(pId);
                                    lastProcessedEdges[pId] = boundaryEdge;

                                }

                                LOGGER.info(Thread.currentThread().getName() + " MASTER.GET_WORKER_STATES");
                                float currentAvgDegree = 0.0f;
                                // update average degree
                                if (MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTH || MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BTHE || MultiThreadedMaster.heuristic == MultiThreadedHeuristic.BH) {
                                    currentAvgDegree = (float)newAvgDegree;
                                }

                                long fetchUpdateStart = System.currentTimeMillis();
                                MultiThreadedMaster.getStateFromWorkers(currentAvgDegree, lastProcessedEdges);
                                long fetchUpdateEnd = System.currentTimeMillis();
                                MultiThreadedMaster.per_sync_time.add((fetchUpdateEnd-fetchUpdateStart));
                                MultiThreadedMaster.compute_totalFetchTime.addAndGet((fetchUpdateEnd-fetchUpdateStart));

                                MultiThreadedMaster.sync_points_avg_degree_change.add("(" + newAvgDegree + "," + globalAvgDegree + ")");
                                MultiThreadedMaster.syncCounter.getAndIncrement();



                                long notifyStart = System.currentTimeMillis();
                                synchronized (MultiThreadedMaster.stateUpdateObj) {
                                    LOGGER.info(Thread.currentThread().getId() + " post sync operations");
                                    LOGGER.info("READ COUNT - " + IOSharedResources.readCount.get() + " CONSUMED COUNT - " + IOSharedResources.consumedCount.get());
                                    MultiThreadedMaster.updateRequired.compareAndSet(true, false);
                                    MultiThreadedMaster.stateUpdateObj.notifyAll();
                                }

                                long notifyEnd = System.currentTimeMillis();
                                MultiThreadedMaster.compute_totalSyncNotifyTime.addAndGet((notifyEnd-notifyStart));

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        if (MultiThreadedMaster.updateRequired.get()) {
                            try {
                                synchronized (MultiThreadedMaster.stateUpdateObj) {
                                    synchronized (MultiThreadedMaster.syncThreadLockPartition) {
                                        MultiThreadedMaster.ACTIVE_THREAD_COUNT--;
                                        MultiThreadedMaster.syncThreadLockPartition.notify();
                                    }
                                    MultiThreadedMaster.stateUpdateObj.wait();

                                    synchronized (MultiThreadedMaster.syncThreadLockPartition) {
                                        MultiThreadedMaster.ACTIVE_THREAD_COUNT++;
                                    }
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                long syncTimeEnd = System.currentTimeMillis();
                MultiThreadedMaster.compute_totalSyncWaitTime.addAndGet((syncTimeEnd-syncTimeStart));
                // END here

            } else {
                if (!IOSharedResources.moreLines.get()) {
                    if (!IOQueue.isEmpty()) {
                        continue;
                    }
                    else {
                        LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " releasing exiting");
                        LOGGER.info("MASTER.COMPUTE.THREAD EDGE COUNT - " + MultiThreadedMaster.processedEdgeCount.get());
                        synchronized (MultiThreadedMaster.syncThreadLockPartition) {
                            MultiThreadedMaster.ACTIVE_THREAD_COUNT--;
                            MultiThreadedMaster.syncThreadLockPartition.notify();
                        }
                        MultiThreadedMaster.computePoolLatch.countDown();
                        break;
                    }
                } else if (IOSharedResources.isReading.get()) {
                    try {
                        long busyWaitStart = System.currentTimeMillis();
                        Thread.sleep(1000);
                        long busyWaitEnd = System.currentTimeMillis();
                        MultiThreadedMaster.compute_totalBusyWaitingTime.addAndGet((busyWaitEnd-busyWaitStart));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // Dynamic IO Reads - when queue sizes reaches below a certain threshold
            long IOReadStart = System.currentTimeMillis();
            if (IOSharedResources.readCount.get() - IOSharedResources.consumedCount.get() <= (((MultiThreadedMaster.batchSize)*((MultiThreadedMaster.threshold)/100))) &&
                    IOSharedResources.moreLines.get() && !IOSharedResources.isReading.get())
            {
                if (!IOSharedResources.isReading.compareAndSet(false, true)) {
                    continue;
                }

                LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " Reached notify condition");

                try {
                    IOSharedResources.consumerNotifyLock.acquire();
                    synchronized (IOSharedResources.producerLock) {
                        LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " notifying reader");
                        IOSharedResources.producerLock.notify();
                    }

                    synchronized (IOSharedResources.consumerLock) {
                        IOSharedResources.consumerNotifyLock.release();
                        LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " releasing lock and waiting");
                        IOSharedResources.consumerLock.wait();
                        LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " resuming");
                    }

                } catch (InterruptedException e) {
                    LOGGER.info("MASTER.COMPUTE.THREAD - " + Thread.currentThread().getId() + " thread interruped");
                    e.printStackTrace();
                }
            }
            long IOReadEnd = System.currentTimeMillis();
            MultiThreadedMaster.compute_IOReadTime.addAndGet((IOReadEnd-IOReadStart));

        }
    }

    @Override
    public void run() {
        long computeStart = System.currentTimeMillis();
        synchronized (MultiThreadedMaster.syncThreadLockPartition) {
            MultiThreadedMaster.ACTIVE_THREAD_COUNT++;
        }
        processEdge();
        long computeEnd = System.currentTimeMillis();
        MultiThreadedMaster.totalComputeTime.addAndGet((computeEnd-computeStart));
        MultiThreadedMaster.computeTimes.add((computeEnd-computeStart));
    }

}
