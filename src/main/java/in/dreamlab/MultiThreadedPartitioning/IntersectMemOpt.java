package in.dreamlab.MultiThreadedPartitioning;

import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IntersectMemOpt {

    IntersectArrayHandler handler;

    // This implementation uses Singleton arrays with max size, mapped to each thread
    // call this once for the entire worker
    public IntersectMemOpt(IntersectArrayHandler _handler) {
        handler = _handler;
    }

    // this captures the contents of the adj list and saves it in a singleton array without additional memory allocation
    // THIS BLOCK THE ACQUIRED ARRAY FROM USAGE BY OTHERS. YOU MUST CALL EITHER INTERSECT() OR INTERSECTSORTBIN() [NOT BOTH] ONCE TO RELEASE THE ARRAY.
    // Call this once per edge intersection.
    public void initArrays(byte threadIndex, TLongHashSet s1, TLongHashSet s2) {
        IntersectArrayHandler.LongArrayPair pair = handler.acquire(threadIndex);
        s1.toArray(pair.arr1);
        s2.toArray(pair.arr2);
        pair.setSizes(s1.size(), s2.size());
    }

    // call this once to perform the actual intersection, O(s1 * s2)
    // this will also release the acquired array
    public List<Long> intersect(byte threadIndex) {
        IntersectArrayHandler.LongArrayPair pair = null;
        try {
            pair = handler.get(threadIndex);

            long[] a1 = pair.arr1;
            long[] a2 = pair.arr2;

            int s1Size = pair.getSize1();
            int s2Size = pair.getSize2();

            List<Long> result = new ArrayList<>(Math.max(s1Size, s2Size));
            // O(n * m)
            for (int i = 0; i < s1Size; i++) {
                for(int j = 0; j < s2Size; j++) {
                    if (a1[i] == a2[j]) {
                        result.add(a1[i]);
                    }
                }
            }

            return result;
        } finally {
            if(pair != null) handler.release(pair);
        }
    }

    // call this once to perform the actual intersection, O((s1 * s2) * log(s1))
    // this will also release the acquired array
    public List<Long> intersectSortBinSearch(byte threadIndex) {

        IntersectArrayHandler.LongArrayPair pair = null;
        try {
            pair = handler.get(threadIndex);
        
            long[] a1 = pair.arr1;
            long[] a2 = pair.arr2;

            int s1Size = pair.getSize1();
            int s2Size = pair.getSize2();

            // O((m+n) * log(n)), where n <= m
            List<Long> result = new ArrayList<>(Math.max(s1Size, s2Size));
            if(s1Size > s2Size){ // swap arrays to ensure a1 is smaller
                long[] tmpa = a2;
                a2 = a1;
                a1 = tmpa;

                int tempSize = s2Size;
                s2Size = s1Size;
                s1Size = tempSize;
            }

            Arrays.sort(a1, 0, s1Size); // sort smaller array (n.log(n)). Account for truncated useful items in array.

            // binary search for items in larger array a2 within smaller array a1 (m.log(n))        
            for(int j = 0; j < s2Size; j++) {
                if (Arrays.binarySearch(a1, 0, s1Size, a2[j]) >= 0) {  // returns non-negative if item found. Account for truncated useful items in array.
                    result.add(a2[j]);
                }
            }

            return result;
        } finally {
            if(pair != null) handler.release(pair);
        }
    }
    
    public long getProduct(byte threadIndex) {
        IntersectArrayHandler.LongArrayPair pair = handler.get(threadIndex);
        
        int s1Size = pair.getSize1();
        int s2Size = pair.getSize2();
        return (s1Size * s2Size);
    }

    public long getSum(byte threadIndex) {
        IntersectArrayHandler.LongArrayPair pair = handler.get(threadIndex);
        
        int s1Size = pair.getSize1();
        int s2Size = pair.getSize2();
        
        return (s1Size + s2Size);
    }
}
