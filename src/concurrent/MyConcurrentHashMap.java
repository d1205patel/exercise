package concurrent;

import sun.misc.Unsafe;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.reflect.Field;

public class MyConcurrentHashMap<K extends Integer,V extends Integer> implements Map<K,V>{

    private static int MAX_CAP = 1<<30;
    private static int DEFAULT_CAP = 16;
    private static float DEFAULT_LOAD_FACTOR = 0.75F;

    private AtomicInteger size = new AtomicInteger();
    private volatile int threshold;
    private float loadFactor;
    private volatile int nextTableLength;
    private volatile int resizingIndex;

    private volatile Bucket<K,V>[] table, nextTable;

    private Object resizeLock = new Object();

    private volatile Set<Entry<K,V>> es;
    private volatile Set<K> ks;
    private volatile Collection<V> values;

    private static final Unsafe U;
    private static final long ABASE;
    private static final int ASHIFT;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            U = (Unsafe) f.get(null);
            Class<?> ak = MyConcurrentHashMap.Bucket[].class;
            ABASE = U.arrayBaseOffset(ak);
            int scale = U.arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0)
                throw new Error("data type scale not a power of two");
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    //<----------------------------------------- Constructors ------------------------------------------>//

    public MyConcurrentHashMap(int cap,float loadFactor) {
        if(cap<0) {
            throw new IllegalArgumentException("Illegal Capacity:" + cap);
        }
        if(loadFactor<=0 || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Illegal load factor" + loadFactor);
        }
        if(cap>MAX_CAP) {
            cap = MAX_CAP;
        }
        this.loadFactor = loadFactor;
        this.threshold = tableSizeForCap(cap);
    }
    public MyConcurrentHashMap(int cap) {
        this(cap,DEFAULT_LOAD_FACTOR);
    }
    public MyConcurrentHashMap() {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
    }

    //<----------------------------------------- Public Methods --------------------------------------->//

    @Override
    public V put(K key,V value) {
        return putVal(key,value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        if(m!=null) {
            for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
                putVal(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        Node<K,V> removedNode = removeNode(key,value,true);
        return removedNode!=null;
    }

    @Override
    public V remove(Object key) {
        Node<K,V> removedNode = removeNode(key,null,false);
        return removedNode==null ? null : removedNode.value;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        V foundValue = get(key);
        if(Objects.equals(oldValue,foundValue)) {
            putVal(key,newValue);
            return true;
        }
        return false;
    }

    @Override
    public V replace(K key, V value) {
        return putVal(key,value);
    }

    @Override
    public void clear() {
        if(table!=null) {
            Arrays.fill(table,null);
            size.set(0);
        }
    }

    @Override
    public V get(Object key) {
        Node<K,V> node = getNode(key);
        return node==null ? null : node.value;
    }

    @Override
    public boolean containsKey(Object key) {
        return getNode(key)!=null;
    }

    @Override
    public boolean containsValue(Object value) {
        if(table!=null) {
            for(Bucket<K,V> bucket:table) {
                Node<K,V> node = bucket.node;
                while(node!=null) {
                    if(Objects.equals(value,node.value)) {
                        return true;
                    }
                    node = node.next;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isEmpty() {
        return size.get()==0;
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean equals(Object o) {
        if(o==this) {
            return true;
        }
        if(o instanceof Map<?,?>) {
            Map<?,?> map = (Map<?,?>)o;
            Set<Entry<K,V>> entrySet = entrySet();
            for(Entry<?,?> entry: map.entrySet()) {
                if(!entrySet.contains(entry)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
    @Override
    public Set<K> keySet() {
        return null;
    }
    @Override
    public Collection<V> values() {
        return null;
    }

    //<--------------------------------- Private Helper methods --------------------------------------->//

    private V putVal(K key,V value) {
        if(table == null || table.length == 0) {
            synchronized (resizeLock) {
                if (table == null || table.length == 0) {
                    nextTableLength = threshold > 0 ? threshold : DEFAULT_CAP;
                    float ft = (float) nextTableLength * loadFactor;
                    threshold = ft >= (float) Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ft;
                    table = (Bucket<K, V>[]) new Bucket[nextTableLength];
                    for(int i=0;i<nextTableLength;i++) {
                        setTabAt(table,i,new Bucket<>());
                    }
                    nextTable = table;
                }
            }
        }
        Lock bucketLock = null;
        while(true) {
            int n = table.length;
            int hash = hash(key);
            int index = hash & (n - 1);

            Node prevNode = getFirstNode(table,index);
            if (prevNode == null && (n==nextTableLength || (2*n==nextTableLength && resizingIndex<=index))) {
                bucketLock = getLock(table,index);
                if(bucketLock==null) {
                    continue;
                }
                bucketLock.lock();
                if (getFirstNode(table,index) == null && (n==table.length || (2*n==nextTableLength && resizingIndex<=index))) {
                    Bucket<K,V> bucket = getBucket(table,index);
                    bucket.node = new Node(hash,key,value,null);
                    setTabAt(table,index,bucket);
//                    System.out.println("Placed at -> " + index + " : " + "K" + value + " , n =" + n + " resizingIndex : " + resizingIndex);
                    break;
                }
            } else if(n*2==nextTableLength && resizingIndex>index && getFirstNode(nextTable,hash &(nextTableLength-1))==null) {
                int nextIndex = hash & (nextTableLength-1);
                bucketLock = getLock(nextTable,nextIndex);
                if(bucketLock==null) {
                    continue;
                }
                bucketLock.lock();
                if(n*2==nextTableLength && getFirstNode(nextTable,nextIndex)==null && resizingIndex>index) {
                    Bucket<K,V> bucket = getBucket(nextTable,nextIndex);
                    bucket.node = new Node(hash,key,value,null);;
                    setTabAt(nextTable,nextIndex,bucket);
//                    System.out.println("Placed at new Table-> " + nextIndex + " : " + "K" + value + " resizingIndex : " + resizingIndex);
                    break;
                }
            } else {
                Node<K, V> foundNode;
                int printIndex = index;
                String prefix = "Placed at -> ";
                boolean addInNextTable = false;
                if(nextTableLength == n*2 && resizingIndex>index) {
                    int nextTableIndex = hash&(nextTableLength-1);
                    prevNode = getFirstNode(nextTable,nextTableIndex);
                    bucketLock = getLock(nextTable,nextTableIndex);
                    printIndex = nextTableIndex;
                    prefix = "Placed at new Table but not first";
                    if(prevNode==null || bucketLock==null) {
                        continue;
                    }
                    addInNextTable = true;
                }
                bucketLock = getLock(table,index);
                if(prevNode==null || bucketLock==null) {
                    continue;
                }
                bucketLock.lock();
                if (hash == prevNode.hash && Objects.equals(key, prevNode.key)) {
                    foundNode = prevNode;
                } else {
                    while ((foundNode = prevNode.next) != null) {
                        if (hash == foundNode.hash && Objects.equals(key, foundNode.key)) {
                            break;
                        }
                        prevNode = prevNode.next;
                    }
                }
                if((n==nextTableLength && resizingIndex<=index) || (addInNextTable && nextTableLength == n*2 && resizingIndex>index)) {
                    if (foundNode != null) {
//                        System.out.println(prefix + printIndex + " : " + value);
                        V oldValue = foundNode.value;
                        foundNode.value = value;
                        bucketLock.unlock();
                        return oldValue;
                    } else {
                        prevNode.next = new Node<>(hash, key, value, null);
//                        System.out.println(prefix + printIndex + " : " + "K" +value + " resizingIndex : " + resizingIndex);
                    }
                    break;
                }

            }
            bucketLock.unlock();
        }
        bucketLock.unlock();
        if (size.incrementAndGet() > threshold) {
            resize();
        }
        return null;
    }

    private Node<K,V> removeNode(Object key,Object value,boolean matchValue) {
        if(table!=null && table.length>0) {
            Lock bucketLock = null;
            while(true) {
                int hash = hash(key);
                int n = table.length;
                int index = hash & (n - 1);
                int nextTableIndex = index;
                boolean searchInNextTable = false;
                Node<K, V> prevNode;
                bucketLock = getLock(table,index);
                if(n*2==nextTableLength && resizingIndex>index) {
                    searchInNextTable = true;
                    nextTableIndex = hash & (nextTableLength-1);
                    bucketLock = getLock(nextTable,nextTableIndex);
                }
                if(bucketLock==null) {
                    continue;
                }
                bucketLock.lock();
                Bucket<K,V>[] tab = table;
                if(n==nextTableLength || (n*2==nextTableIndex && resizingIndex<=index)) {
                    prevNode = getFirstNode(table,index);
                } else {
                    nextTableIndex = hash & (nextTableLength-1);
                    prevNode = getFirstNode(nextTable,nextTableIndex);
                    tab = nextTable;
                }
                if((n==nextTableLength || (n*2==nextTableIndex && resizingIndex<=index)) || (searchInNextTable && (n*2==nextTableLength && resizingIndex>index))) {
                    if (prevNode != null) {
                        Node<K, V> foundNode;
                        if (hash == prevNode.hash && Objects.equals(key, prevNode.key)) {
                            foundNode = prevNode;
                        } else {
                            while ((foundNode = prevNode.next) != null) {
                                if (hash == foundNode.hash && Objects.equals(key, foundNode.key)) {
                                    break;
                                }
                                prevNode = prevNode.next;
                            }
                        }
                        if (foundNode != null && (!matchValue || Objects.equals(value, foundNode.value))) {
                            if (prevNode == foundNode) {
                                Bucket<K,V> bucket = getBucket(tab,nextTableIndex);
                                bucket.node = foundNode.next;
                                setTabAt(tab,nextTableIndex,bucket);
                            } else {
                                prevNode.next = foundNode.next;
                            }
                            size.decrementAndGet();
                            foundNode.next = null;
                            bucketLock.unlock();
                            return foundNode;
                        }
                    } else {
                        break;
                    }
                }
                bucketLock.unlock();
            }
            bucketLock.unlock();
        }
        return null;
    }

    private Node<K,V> getNode(Object key) {
        if(table!=null && table.length!=0) {
            Lock bucketLock = null;
            while(true) {
                int h = hash(key);
                int n = table.length;
                int index = h & (n - 1);
                int nextTableIndex = index;
                boolean searchInNextTable = false;
                Node<K,V> node = null;
                bucketLock = getLock(table,index);
                if(n*2==nextTableLength && resizingIndex>index) {
                    searchInNextTable = true;
                    nextTableIndex = h & (nextTableLength-1);
                    bucketLock = getLock(nextTable,nextTableIndex);
                }
                if(bucketLock==null) {
                    continue;
                }
                bucketLock.lock();
                if(n==nextTableLength || (n*2==nextTableIndex && resizingIndex<=index)) {
                    node = getFirstNode(table,index);
                } else {
                    node = getFirstNode(nextTable,nextTableIndex);
                }
                if((n==nextTableLength || (n*2==nextTableIndex && resizingIndex<=index)) || (searchInNextTable && (n*2==nextTableLength && resizingIndex>index))) {
                    while (node != null) {
                        if (h == hash(node.key) && Objects.equals(key, node.key)) {
                            bucketLock.unlock();
                            return node;
                        }
                        node = node.next;
                    }
                    break;
                } else {
                    bucketLock.unlock();
                }
            }
            bucketLock.unlock();
        }
        return null;
    }

    private void resize() {
        if (size.get() < threshold || table.length == MAX_CAP) {
            return;
        }
        synchronized (resizeLock) {
            if (size.get() < threshold || table.length == MAX_CAP) {
                return;
            }
            int oldCap = table.length;
            if (oldCap > 0) {
                if (oldCap == MAX_CAP) {
                    return;
                } else {
                    nextTableLength = oldCap << 1;
                }
            }
            float ft = (float) nextTableLength * loadFactor;
            threshold = ft >= (float) Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ft;

//            System.out.println("Resizing table to length : " + nextTableLength);

            nextTable = (Bucket<K, V>[]) new Bucket[nextTableLength];
            for(int i=0;i<nextTableLength;i++) {
                setTabAt(nextTable,i,new Bucket<>());
            }
            resizingIndex = 0;
            for (int i = 0; i < oldCap; i++) {
                Lock bucketLock=getLock(table,i);;
                if(bucketLock==null) {
                    i--;
                    continue;
                }
                bucketLock.lock();
                Node<K, V> node = getFirstNode(table, i);
                if (node != null) {
                    Node<K, V> highHead = null, highTail = null, lowHead = null, lowTail = null;
                    while (node != null) {
                        if ((node.hash & oldCap) == 0) {
//                            System.out.println("Not Moving " + "K" + node.value + "  ," + (node.hash & (oldCap-1)) + " to " + (node.hash & (nextTableLength-1)) + "oldcap" + oldCap + " resizingIndex : " + resizingIndex);
                            if (lowHead == null) {
                                lowHead = lowTail = node;
                            } else {
                                lowTail = lowTail.next = node;
                            }
                        } else {
//                            System.out.println("Moving : " + "K" + node.value + "  ,"+ (node.hash & (oldCap-1)) + " to " + (node.hash & (nextTableLength-1))+ "oldcap" + oldCap  +" resizingIndex : " + resizingIndex);
                            if (highHead == null) {
                                highHead = highTail = node;
                            } else {
                                highTail = highTail.next = node;
                            }
                        }
                        node = node.next;
                    }
                    if (lowHead != null) {
                        Bucket<K,V> bucket = getBucket(nextTable,i);
                        lowTail.next = null;
                        bucket.node = lowHead;
                        setTabAt(nextTable,i,bucket);
                    }
                    if (highHead != null) {
                        Bucket<K,V> bucket = getBucket(nextTable,i+oldCap);
                        highTail.next = null;
                        bucket.node = highHead;
                        setTabAt(nextTable,i+oldCap,bucket);
                    }
                }
                resizingIndex++;
                bucketLock.unlock();
            }
            table = nextTable;
            resizingIndex = -1;
        }
    }

    //<----------------------------------- static utilities --------------------------------->//

    private static int tableSizeForCap(int cap) {
        int n = cap-1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }

    private int hash(Object key) {
        return key==null ? 0 : (Integer) key;
    }

    private static <K,V> Node<K,V> getFirstNode(Bucket<K,V>[] table,int index) {
        Bucket<K,V> bucket = getBucket(table,index);
        return bucket==null ? null : getBucket(table,index).node;
    }

    private static <K,V> Bucket<K,V> getBucket(Bucket<K,V>[] table, int index) {
        return (MyConcurrentHashMap.Bucket<K,V>)U.getObjectVolatile(table, ((long)index << ASHIFT) + ABASE);
    }

    static final <K,V> void setTabAt (Bucket<K,V>[] tab, int i, Bucket<K,V> v) {
        U.putObjectVolatile(tab, ((long)i << ASHIFT) + ABASE, v);
    }

    private static <K,V> Lock getLock(Bucket<K,V>[] table,int index) {
        Bucket<K,V> bucket = getBucket(table,index);
        return bucket==null ? null:getBucket(table,index).bucketLock ;
    }

    private static class Node<K,V> implements Map.Entry<K,V> {
        final int hash;
        final K key;
        V value;
        Node<K,V> next;

        Node(int hash,K key,V value,Node<K,V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }

        @Override
        public int hashCode() {
            return hash ;
        }

        @Override
        public boolean equals(Object o) {
            if(o instanceof Entry<?,?>) {
                Entry<?,?> entry = (Entry<?,?>) o;
                return Objects.equals(key,entry.getKey()) && Objects.equals(value,entry.getValue());
            }
            return false;
        }
    }

    private static class Bucket<K,V> {
        Node<K,V> node;
        Lock bucketLock;

        Bucket() {
            bucketLock = new ReentrantLock();
        }
    }

}