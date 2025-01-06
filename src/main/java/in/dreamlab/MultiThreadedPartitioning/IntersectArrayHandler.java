package in.dreamlab.MultiThreadedPartitioning;

import java.util.concurrent.atomic.AtomicBoolean;

public class IntersectArrayHandler {
    public class LongArrayPair {
        private static final int DEFAULT_CAPACITY = 1000;
        
        public LongArrayPair(byte _index) {
            this(DEFAULT_CAPACITY, _index);
        }

        public LongArrayPair(int _capacity, byte _index) {
            capacity = _capacity;
            index = _index;

            arr1 = new long[capacity];
            arr2 = new long[capacity];
            isAcquired = new AtomicBoolean(false);
        }

        public void setSizes(int _s1, int _s2) {
            size1 = _s1;
            size2 = _s2;
        }

        public int getSize1(){
            return size1;            
        }

        public int getSize2(){
            return size2;            
        }

        final long[] arr1;
        final long[] arr2;
        int size1 = -1, size2 = -1;

        private final int capacity;
        private final byte index; // index in parent array where this array will be placed
        private final AtomicBoolean isAcquired;
    }

    
    private static final byte MAX_ARRAYS = 32;    
    final byte maxArrays; 
    final int maxCapacity;    
    final LongArrayPair[] arrayItems;

    // Constructor is not thread safe
    public IntersectArrayHandler(byte _maxArrays, int _maxCapacity){
        maxArrays = _maxArrays;
        maxCapacity = _maxCapacity;

        arrayItems = new LongArrayPair[maxArrays];
        for(byte i=0; i<maxArrays; i++){
            arrayItems[i] = new LongArrayPair(maxCapacity, i);
        }
    }

    public LongArrayPair acquire(byte index){
        // must be accessed by only a single thread
        if(arrayItems[index].isAcquired.compareAndSet(false, true)) {
            return arrayItems[index];
        } else {
            throw new RuntimeException("Attempting to acquire an array with index " + index + " which has already been acquired and not released yet");
        }
    }

    public LongArrayPair get(byte index){
        if(arrayItems[index].isAcquired.get()) {
            return arrayItems[index];
        } else {
            throw new RuntimeException("Attempting to get an array with index " + index + " which has not been acquired yet");
        }
    }

    public void release(LongArrayPair arr){
        // must be accessed by only a single thread
        arrayItems[arr.index].setSizes(-1, -1);

        if(!arrayItems[arr.index].isAcquired.compareAndSet(true, false)) {
            throw new RuntimeException("Attempting to release an array with index " + arr.index + " which has already been released and not acquired");
        }

    }
}
