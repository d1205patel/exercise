package collection_framework;

import java.util.*;

public class MyHashMap<K,V> implements Map<K,V>{

    private static int MAX_CAP = 1<<30;
    private static int DEFAULT_CAP = 16;
    private static float DEFAULT_LOAD_FACTOR = 0.75F;

    private int size;
    private int threshold;
    private float loadFactor;
    private int modCount;

    private Node<K,V>[] table;

    private Set<Entry<K,V>> es;
    private Set<K> ks;
    private Collection<V> values;

    //<----------------------------------------- Constructors ------------------------------------------>//

    MyHashMap(int cap,float loadFactor) {
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
    MyHashMap(int cap) {
        this(cap,DEFAULT_LOAD_FACTOR);
    }
    MyHashMap() {
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
            for(Node<K,V> node:table) {
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
        return size==0;
    }

    @Override
    public int size() {
        return size;
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

    //<------------------------- Methods that returns view(mutable) of HashMap ---------------------------->//

    @Override
    public Set<Entry<K, V>> entrySet() {
        return es==null ? es = new EntrySet() : es;
    }

    @Override
    public Set<K> keySet() {
        return ks==null ? ks = new KeySet() : ks;
    }

    @Override
    public Collection<V> values() {
        return values==null ? values = new Values() : values;
    }

    //<----------------------------------------- Views of HashMap----------------------------------------->//

    private abstract class CommonHashMapOperation {
        public int size() {
            return size;
        }
        public boolean isEmpty() {
            return size==0;
        }
        public void clear() {
            MyHashMap.this.clear();
        }
        public abstract boolean contains(Object o);
        public boolean containsAll(Collection<?> c) {
            for(Object o: c) {
                if(!contains(o)) {
                    return false;
                }
            }
            return true;
        }
        public abstract boolean remove(Object o);
        public boolean removeAll(Collection<?> c) {
            boolean changed = false;
            for(Object o:c) {
                changed |= remove(o);
            }
            return changed;
        }
        public boolean retainAll(Collection<?> c) {
            boolean changed = false;
            Iterator<?> it = iterator();
            while(it.hasNext()) {
                boolean present = false;
                for(Object o : c) {
                    if(o.equals(it.next())) {
                        present = true;
                        break;
                    }
                }
                if(!present) {
                    changed = true;
                    it.remove();
                }
            }
            return changed;
        }
        public abstract Iterator<?> iterator();
        public Object[] toArray() {
            Iterator<?> it = iterator();
            Object[] array = new Object[size];
            int i = 0;
            while(it.hasNext()) {
                array[i++] = it.next();
            }
            return array;
        }
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] a) {
            if(a.length < size) {
                return (T[])toArray();
            }
            int i = 0;
            Iterator<?> it = iterator();
            while(it.hasNext()) {
                a[i++] = (T)it.next();
            }
            if(i < size) {
                a[i] = null;
            }
            return a;
        }
    }

    private class EntrySet extends CommonHashMapOperation implements Set<Entry<K,V>> {

        @Override
        public boolean contains(Object o) {
            if(o instanceof Entry<?,?>) {
                Entry<?,?> entry = (Entry<?,?>) o;
                Object key = entry.getKey();
                Object value = entry.getValue();
                return Objects.equals(value,get(key));
            }
            return false;
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntrySetItr();
        }

        @Override
        public boolean add(Entry<K, V> kvEntry) {
            K key = kvEntry.getKey();
            V value = kvEntry.getValue();
            if(Objects.equals(value,get(key))) {
                return false;
            }
            putVal(key,value);
            return true;
        }

        @Override
        public boolean remove(Object o) {
            if(o instanceof Entry<?,?>) {
                Entry<?,?> entry = (Entry<?,?>)o;
                return MyHashMap.this.remove(entry.getKey(),entry.getValue());
            }
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            for(Entry<K,V> entry: c) {
                putVal(entry.getKey(),entry.getValue());
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            for(Entry<K,V> entry:this) {
                hashCode += entry.hashCode();
            }
            return hashCode;
        }

    }

    private class KeySet extends CommonHashMapOperation implements Set<K> {

        @Override
        public boolean contains(Object o) {
            return getNode(o)!=null;
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyItr();
        }

        @Override
        public boolean add(K k) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            Node<K,V> removedNode = removeNode(o,null,false);
            return removedNode!=null;
        }

        @Override
        public boolean addAll(Collection<? extends K> c) {
            throw new UnsupportedOperationException();
        }

    }

    private class Values extends CommonHashMapOperation implements Collection<V> {

        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueItr();
        }

        @Override
        public boolean add(V v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            Iterator<V> it = iterator();
            while(it.hasNext()) {
                V value = it.next();
                if(Objects.equals(value,o)) {
                    it.remove();
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            boolean changed = false;
            for(Object o : c) {
                Iterator<V> it = iterator();
                while(it.hasNext()) {
                    V value = it.next();
                    if(Objects.equals(value,o)) {
                        it.remove();
                        changed = true;
                    }
                }
            }
            return changed;
        }

    }

    //<------------------------------------------- Iterators --------------------------------------------->//

    private class HashItr {
        Node<K,V> currentNode,nextNode;
        int expectedModCount;
        int index;

        HashItr() {
            currentNode = null;
            if(table!=null) {
                int n = table.length ;
                index = 0;
                expectedModCount = modCount;
                while(index < n && (nextNode=table[index++])==null);
            }
        }

        public boolean hasNext() {
            return nextNode!=null;
        }

        public Entry<K, V> nextNode() {
            checkForConcurrentModification();
            if(nextNode==null) {
                throw new NoSuchElementException();
            }
            currentNode = nextNode;
            if((nextNode = nextNode.next)==null) {
                int n = table.length;
                while(index < n && (nextNode=table[index++])==null);
            }
            return currentNode;
        }

        public void remove() {
            checkForConcurrentModification();
            if(currentNode==null) {
                throw new IllegalStateException();
            }
            MyHashMap.this.remove(currentNode.key);
            currentNode = null;
            expectedModCount = modCount;
        }

        private void checkForConcurrentModification() {
            if(expectedModCount != modCount) {
                throw new ConcurrentModificationException();
            }
        }
    }

    private class EntrySetItr extends HashItr implements Iterator<Entry<K,V>> {
        @Override
        public Entry<K,V> next() {
            return nextNode();
        }
    }

    private class KeyItr extends HashItr implements Iterator<K> {
        @Override
        public K next() {
            return nextNode().getKey();
        }
    }

    private class ValueItr extends HashItr implements Iterator<V> {
        @Override
        public V next() {
            return nextNode().getValue();
        }
    }

    //<--------------------------------- Private Helper methods --------------------------------------->//

    private V putVal(K key,V value) {
        if(table == null || table.length == 0) {
            resize();
        }
        int n = table.length;
        int hash = hash(key);
        int index = hash & (n-1);
        if(table[index]==null) {
            table[index] = new Node<>(hash, key, value, null);
        } else {
            Node<K,V> foundNode ,prevNode = table[index];
            if(hash==prevNode.hash && Objects.equals(key,prevNode.key)) {
                foundNode = prevNode;
            } else {
                while ((foundNode = prevNode.next) != null) {
                    if (hash == foundNode.hash && Objects.equals(key, foundNode.key)) {
                        break;
                    }
                    prevNode = prevNode.next;
                }
            }
            if(foundNode!=null) {
                V oldValue = foundNode.value;
                foundNode.value = value;
                return oldValue;
            } else {
                prevNode.next = new Node<>(hash,key,value,null);
            }
        }
        if(++size>threshold) {
            modCount++;
            resize();
        }
        return null;
    }

    private Node<K,V> removeNode(Object key,Object value,boolean matchValue) {
        if(table!=null && table.length>0) {
            int hash = hash(key);
            int index = hash & (table.length-1);
            if(table[index]!=null) {
                Node<K,V> prevNode = table[index],foundNode;
                if(hash==prevNode.hash && Objects.equals(key,prevNode.key)) {
                    foundNode = prevNode;
                } else {
                    while ((foundNode = prevNode.next) != null) {
                        if (hash == foundNode.hash && Objects.equals(key, foundNode.key)) {
                            break;
                        }
                        prevNode = prevNode.next;
                    }
                }
                if(foundNode!=null && (!matchValue || Objects.equals(value,foundNode.value))) {
                    if(prevNode==foundNode) {
                        table[index] = foundNode.next;
                    } else {
                        prevNode.next = foundNode.next;
                    }
                    size--;
                    modCount++;
                    foundNode.next=null;
                    return foundNode;
                }
            }
        }
        return null;
    }

    private Node<K,V> getNode(Object key) {
        if(table!=null && table.length!=0) {
            int h = hash(key);
            int index = h & (table.length -1);
            if(table[index]!=null) {
                Node<K,V> node = table[index];
                while(node != null) {
                    if(h == hash(node.key) && Objects.equals(key,node.key)) {
                        return node;
                    }
                    node = node.next;
                }
            }
        }
        return null;
    }

    private void resize() {
        int oldCap = table==null ? 0: table.length;
        int newCap;
        if(oldCap>0) {
            if(oldCap==MAX_CAP) {
                return;
            } else {
                newCap = oldCap<<1;
            }
        } else if(threshold>0) {
            newCap = threshold;
        } else {
            newCap = DEFAULT_CAP;
        }
        float ft = (float)newCap*loadFactor;
        threshold = ft >= (float)Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)ft;

        @SuppressWarnings("unchecked")
        Node<K,V>[] newTable = (Node<K,V>[])new Node[newCap];
        for(int i=0;i<oldCap;i++) {
            if(table[i]!=null) {
                Node<K,V> node = table[i];
                Node<K,V> highHead=null,highTail=null,lowHead=null,lowTail=null;
                while(node!=null) {
                    if((node.hash & oldCap)==0) {
                        if(lowHead==null) {
                            lowHead = lowTail = node;
                        } else {
                            lowTail = lowTail.next = node;
                        }
                    } else {
                        if(highHead==null) {
                            highHead = highTail = node;
                        } else {
                            highTail = highTail.next = node;
                        }
                    }
                    node = node.next;
                }
                if(lowHead!=null) {
                    newTable[i] = lowHead;
                    lowTail.next = null;
                }
                if(highHead!=null) {
                    newTable[i + oldCap] = highHead;
                    highTail.next = null;
                }
            }
        }
        table = newTable;
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

    private static int hash(Object key) {
        return key==null ? 0 : key.hashCode();
    }

    private static class Node<K,V> implements Entry<K,V>{
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
            return hash ^ hash(value);
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
}