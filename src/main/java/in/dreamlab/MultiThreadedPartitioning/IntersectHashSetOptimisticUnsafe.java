package in.dreamlab.MultiThreadedPartitioning;

import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.iterator.TLongIterator;

import java.util.ArrayList;
import java.util.List;

public class IntersectHashSetOptimisticUnsafe {
    private TLongHashSet a1;
    private TLongHashSet a2;

    private int s1Size;
    private int s2Size;

     public IntersectHashSetOptimisticUnsafe(TLongHashSet s1, TLongHashSet s2) {
        s1Size = s1.size();
        s2Size = s2.size();

        if(s1Size <= s2Size){ // ensure a1 is smaller set
            a1 = s1;
            a2 = s2;
        } else {
            s1Size = s2.size();
            s2Size = s1.size();
            a1 = s2;
            a2 = s1;
        }
    }

    public List<Long> intersect() {
        List<Long> result = new ArrayList<>(Math.max(s1Size, s2Size));
        TLongIterator iter1 = a1.iterator();

        // O(s1Size) | s1Size < s2Size
        while (iter1.hasNext()) { // iterate over smaller set a1, s1Size items
            long item1 = iter1.next();
            // find presence of item in a1 within a2
            // FIXME: not thread safe
            if(a2.contains(item1)) {  // O(1) expected
                result.add(item1);
            }
        }

        return result;
    }

    
    public long getProduct() {
        return (s1Size*s2Size);
    }

    public long getSum() {
        return (s1Size+s2Size);
    }

}
