package collection_framework;

import java.util.*;

public class MyHashMap<K,V> extends AbstractMap<K,V> implements Map<K,V>{

    private static int MAX_CAP = 1<<30;
    private static int DEFAULT_CAP = 16;
    private static float DEFAULT_LOAD_FACTOR = 0.75F;

    private int size;
    private int threshold;
    private float loadFactor;

    private Node<K,V>[] table;

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
    public boolean remove(Object key, Object value) {
        return Objects.equals(removeNode(key,value,true),value);
    }

    @Override
    public V remove(Object key) {
        return removeNode(key,null,false);
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
            for(int i=0;i<table.length;i++)
                table[i] = null;
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
            for(int i=0;i<table.length;i++) {
                Node<K,V> node = table[i];
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
    public Set<Entry<K, V>> entrySet() {
        return null;
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
            table[index] = new Node(hash,key,value,null);
        } else {
            Node<K,V> foundNode = null;
            Node<K,V> prevNode = table[index];
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
                prevNode.next = new Node(hash,key,value,null);
            }
        }
        if(++size>threshold) {
            resize();
        }
        return null;
    }

    private V removeNode(Object key,Object value,boolean matchValue) {
        if(table!=null && table.length>0) {
            int hash = hash(key);
            int index = hash & (table.length-1);
            if(table[index]!=null) {
                Node<K,V> prevNode = table[index],foundNode=null;
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
                    V oldValue = foundNode.value;
                    if(prevNode==foundNode) {
                        table[index] = foundNode.next;
                    } else {
                        prevNode.next = foundNode.next;
                    }
                    size--;
                    return oldValue;
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
        int newCap = 0;
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
        return (n < 0) ? 1 : (n >= MAX_CAP) ? MAX_CAP : n + 1;
    }

    private static int hash(Object key) {
        return key==null ? 0 : key.hashCode();
    }

    private class Node<K,V> {
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

    }

}