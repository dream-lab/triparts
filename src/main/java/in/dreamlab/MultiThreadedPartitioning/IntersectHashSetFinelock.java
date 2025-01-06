package in.dreamlab.MultiThreadedPartitioning;

import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.iterator.TLongIterator;

import java.util.ArrayList;
import java.util.List;

public class IntersectHashSetFinelock {
    private TLongHashSet s1;
    private TLongHashSet s2;

    private int s1Size, s2Size;

    private long v1ID, v2ID;

    // This pessimistic by doing vertex level locking of adj list for source/sink vertices, here and in parent code
    // Always acquire lock for smaller vertex id before larger vertex ID, here and in parent code, to prevent deadlock!!!!
     public IntersectHashSetFinelock(TLongHashSet s1_, TLongHashSet s2_, long v1ID_, long v2ID_) {
        s1Size = s1_.size();
        s2Size = s2_.size();

        if(s1Size <= s2Size){ // ensure s1 is smaller set to help with performance
            s1 = s1_;
            s2 = s2_;
            v1ID = v1ID_;
            v2ID = v2ID_;

        } else {
            s1Size = s2_.size();
            s2Size = s1_.size();
            s1 = s2_;
            s2 = s1_;
            v1ID = v2ID_;
            v2ID = v1ID_;
        }
    }

    public List<Long> intersect() {
        List<Long> result = new ArrayList<>(Math.max(s1Size, s2Size));

        if(v1ID < v2ID) { // ALWAYS get lock on smaller vertex ID before larger to avoid deadlocks
            synchronized(s1) {
                synchronized(s2) {
                    doCompare(result);
                }
            }
        } else {
            synchronized(s2) {
                synchronized(s1) {
                    doCompare(result);
                }
            }

        }

        return result;
    }

    /**
     * Acquire lock to s1 and s2 before calling this function
     * 
     * @param result
     */
    private void doCompare(List<Long> result){
        TLongIterator iter1 = s1.iterator();

        // O(s1Size) | s1Size < s2Size
        while (iter1.hasNext()) { // iterate over smaller set s1, s1Size items
            long item1 = iter1.next();
            // find presence of item in s1 within s2
            if(s2.contains(item1)) {  // O(1) expected
                result.add(item1);
            }
        }        
    }
    
    public long getProduct() {
        return (s1Size*s2Size);
    }

    public long getSum() {
        return (s1Size+s2Size);
    }

}
