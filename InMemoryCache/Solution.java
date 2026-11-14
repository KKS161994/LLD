package InMemoryCache;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import InMemoryCache.Solution.CacheKey;
import InMemoryCache.Solution.CacheValue;
import InMemoryCache.Solution.EvictionPolicy;

public class Solution {
    /***
     * 
     * 
     * Interview problem
     * 
     * Design and implement an in-memory caching library that applications can use
     * to store and retrieve key-value pairs.
     * 
     * The cache should support configurable capacity and eviction policies.
     * 
     * Your implementation should be extensible enough to introduce new eviction
     * strategies without rewriting the core cache.
     * Interview format — 60 minutes
     * Phase Time
     * Requirements clarification and flows 8 min
     * Entities, responsibilities, invariants 7 min
     * Interfaces and class design 5 min
     * Independent Java implementation 30 min
     * Validation and edge cases 10 min
     * 
     * 
     * Clarifying question
     * 
     * 1. Which eviction policies are required?
     * 3. Is the cache accessed concurrently by multiple threads?
     * 4. What should happen when a key is updated?
     * 5. What happens when an entry expires but hasn't been accessed?
     * 6. What are the expected time complexities of operations?
     * 7: Is there a specific type of data that will be stored or
     * it can be anything for key and value
     * 
     * 
     * 
     * 8: Is there retention period for every key value
     * 
     * 9: If there is retention period will ever access either get or put update it
     * 
     * 10: Ideally for now we are considering get and put anything else
     * 11. What happens when the cache reaches capacity?
     * 
     * 
     * 
     * Reqauirements
     * 
     * 1 Which eviction policies? LRU and FIFO, configurable when creating the
     * cache.
     * 2 Concurrent access? Yes. Multiple threads may call cache operations
     * concurrently.
     * 3 What happens when a key is updated? Replace its value and reset its TTL. It
     * must not increase the number of entries.
     * 4 Expired but never accessed? Lazy expiration is acceptable. Expired entries
     * must not be returned, and capacity reclamation must account for expiration.
     * 5 Expected complexity? Target O(1) average get, put, and remove, excluding
     * expiration cleanup.
     * 6 Key/value types? Generic Cache<K,V>. Keys and values must be non-null. Keys
     * require stable equals() and hashCode().
     * 7 Retention period? Optional TTL per entry. Without TTL, entries remain until
     * eviction, replacement, or removal.
     * 8 Does access refresh TTL? get() does not. put() resets TTL using the newly
     * supplied TTL.
     * 9 Supported operations? get, put, remove, and size.
     * 10 Cache reaches capacity? Reclaim expired entries first where practical; if
     * still full, evict according to the configured policy.
     * 11 Capacity configuration? Fixed positive capacity specified at construction.
     * No dynamic resizing required.
     * 
     * 
     * Completed at 10 minutes
     * 
     * 
     * 
     * Operation LRU FIFO
     * Insert new key Becomes most recently used Becomes newest entry
     * get() existing key Updates recency Does not change order
     * put() existing key Updates recency Moves key to newest position
     * remove() Removes from ordering Removes from ordering
     * Eviction Removes least recently used Removes oldest entry in current FIFO
     * order
     * 
     * flows
     * 
     * 
     * put()
     * 1: CLient enter key and value
     * 2: Remove expired entry if any
     * 2: Update the value of key if it exist if TTL is provide update expiry to
     * current + TTL else no expiry
     * 3: If it doesnt exist capcity is full use policy configure for eviction LRU
     * and FIFO
     * 
     * remove()
     * 1: In case of remove if map contains entry and is not expired remove it else
     * if present but expired just remove it and last return boolean not present
     * 
     * 
     * get()
     * 1: if value is miss return not found
     * 2: if value exist validate its expiry if expiry is configured if it has not
     * expired then onlt return it update its recency and then get answer if expired
     * remove it form map
     * \
     * 
     * 
     * invariants
     * 1: Number of element should not exceed configurable capacity
     * 2: expired element should not be eligible for put get and sizr
     * 3: concurrent oepration should not impact ordering ideally we can use
     * concurrent hashmpa
     * but here recency and removing expired elements
     * Hence synchronised is very importasnt
     * 
     * 22 minutes
     * 
     * 
     * Phase 3 — Class and interface design
     * 
     * CacheKey
     * Object key
     * - long timeStamp
     * override equals and hashCode
     * order
     * 
     * Cache value
     * Object
     * 
     * 
     * enum Policy{
     * FIFO,
     * LRU,
     * }
     * 
     * interface EvictionPolicy<K> {
     * void onInsert(K key);
     * void onAccess(K key);
     * void onUpdate(K key);
     * void onRemove(K key);
     * K evictCandidate();
     * }
     * 
     * LRUEvictionPolicy implement EvictionPolicy
     * 
     * 
     * 
     * FIFO EvictionPolicy implement EvictionPolicy
     * 
     * 
     * 
     * InMemoryCache
     * constructor for policy and capacity
     * get()
     * put()
     * remove()
     * size()
     * 
     * 
     * 38:44
     * 
     * * CacheKey
     * Object key
     * - long timeStamp
     * override equals and hashCode
     * order
     * 
     * 
     * 
     */

    static record CacheKey(Object key) {
        @Override
        public final boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (!(obj instanceof CacheKey other))
                return false;

            return Objects.equals(this.key, other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }
    }

    static record CacheEntry<V>(
            V value,
            long expiresAtMillis) {
        boolean isExpired(long currentTime) {
            return currentTime >= expiresAtMillis;
        }
    }

    interface EvictionPolicy {
        void onInsert(CacheKey key);

        void onAccess(CacheKey key);

        void onUpdate(CacheKey key);

        void onRemove(CacheKey key);

        CacheKey evictCandidate();
    }

    static class Node {
        CacheKey key;
        Node next;
        Node prev;

        Node(CacheKey key, Node next, Node prev) {
            this.key = key;
            this.prev = prev;
            this.next = next;
        }
    }

    static class LRUEvictionPolicy implements EvictionPolicy {

        int capacity;
        Map<CacheKey, Node> cacheMap;
        Node root;
        Node tail;

        LRUEvictionPolicy(int capacity) {
            this.capacity = capacity;
            cacheMap = new HashMap<>();
            this.root = new Node(new CacheKey("root"), null, null);
            this.tail = new Node(new CacheKey("tail"), null, null);
            root.next = tail;
            tail.prev = root;
        }

        @Override
        public void onInsert(CacheKey key) {
            Node node;
            if (cacheMap.containsKey(key)) {
                node = cacheMap.get(key);
                node.prev.next = node.next;
                node.next.prev = node.prev;
                node.next = null;
                node.prev = null;
            } else {
                cacheMap.put(key, new Node(key, null, null));
            }
            node = cacheMap.get(key);
            tail.prev.next = node;
            node.prev = tail.prev;
            node.next = tail;
            tail.prev = node;
        }

        @Override
        public void onAccess(CacheKey key) {
            Node node = cacheMap.get(key);
            node.prev.next = node.next;
            node.next.prev = node.prev;
            tail.prev.next = node;
            node.prev = tail.prev;
            node.next = tail;
            tail.prev = node;
        }

        @Override
        public void onUpdate(CacheKey key) {
            throw new UnsupportedOperationException("Unimplemented method 'onUpdate'");
        }

        @Override
        public void onRemove(CacheKey key) {
            Node node = cacheMap.get(key);
            node.prev.next = node.next;
            node.next.prev = node.prev;
            cacheMap.remove(key);
        }

        @Override
        public CacheKey evictCandidate() {
            if (root.next == tail)
                return null;
            Node node = root.next;
            root.next = node.next;
            node.next.prev = root;
            cacheMap.remove(node.key);
            return node.key;

        }

    }

    static class FIFOEvictionPolicy implements EvictionPolicy {

        int capacity;
        Map<CacheKey, Node> cacheMap;
        Node root;
        Node tail;

        FIFOEvictionPolicy(int capacity) {
            this.capacity = capacity;
            cacheMap = new HashMap<>();
            this.root = new Node(new CacheKey("root"), null, null);
            this.tail = new Node(new CacheKey("tail"), null, null);
            root.next = tail;
            tail.prev = root;
        }

        @Override
        public void onInsert(CacheKey key) {
            Node node;
            if (cacheMap.containsKey(key)) {
                node = cacheMap.get(key);
                node.prev.next = node.next;
                node.next.prev = node.prev;
                node.next = null;
                node.prev = null;
            } else {
                cacheMap.put(key, new Node(key, null, null));
            }
            node = cacheMap.get(key);
            tail.prev.next = node;
            node.prev = tail.prev;
            node.next = tail;
            tail.prev = node;
        }

        @Override
        public void onAccess(CacheKey key) {
            // Nothing need to be done since order remain same
        }

        @Override
        public void onUpdate(CacheKey key) {
            throw new UnsupportedOperationException("Unimplemented method 'onUpdate'");
        }

        @Override
        public void onRemove(CacheKey key) {
            Node node = cacheMap.get(key);
            node.prev.next = node.next;
            node.next.prev = node.prev;
            cacheMap.remove(key);
        }

        @Override
        public CacheKey evictCandidate() {
            if (root.next == tail)
                return null;
            Node node = root.next;
            root.next = node.next;
            node.next.prev = root;
            cacheMap.remove(node.key);
            return node.key;
        }

    }

    static class InMemoryCache {
        EvictionPolicy evictionPolicy;
        int capacity;
        Map<CacheKey, CacheEntry<Object>> map;

        InMemoryCache(int capacity, EvictionPolicy evictionPolicy) {
            this.capacity = capacity;
            this.evictionPolicy = evictionPolicy;
            map = new HashMap<>();
        }

        Object get(Object cacheKey) {
            CacheKey key = new CacheKey(cacheKey);
            if (!map.containsKey(key))
                return null;
            if (map.get(key).isExpired(getCurrentTime())) {
                evictionPolicy.onRemove(key);
                map.remove(key);
                return null;
            }
            CacheEntry<Object> value = map.get(key);
            evictionPolicy.onAccess(key);
            return value.value;
        }

        void put(Object cacheKey, Object value, int TTL) {
            CacheKey key = new CacheKey(cacheKey);
            CacheEntry<Object> cacheValue = new CacheEntry<Object>(value,
                    TTL == 0 ? Long.MAX_VALUE : TTL + getCurrentTime());
            if (map.containsKey(key) || map.size() != capacity) {
                map.put(key, cacheValue);
                evictionPolicy.onInsert(key);
                return;
            }
            validateAndRemoveKeysForExpiry();
            if (map.size() != capacity) {
                map.put(key, cacheValue);
                evictionPolicy.onInsert(key);
                return;
            }
            CacheKey keyToExpire = evictionPolicy.evictCandidate();
            if (keyToExpire == null) {
                throw new UnknownError("Check the policy for error");
            }
            evictionPolicy.onInsert(key);
            map.remove(keyToExpire);
            map.put(key, cacheValue);
            return;
        }

        boolean remove(Object cacheKey) {
            CacheKey key = new CacheKey(cacheKey);
            if (!map.containsKey(key))
                return false;
            evictionPolicy.onRemove(key);
            CacheEntry<Object> value = map.get(key);
            map.remove(key);
            if (value.isExpired(getCurrentTime())) {
                map.remove(key);
                return false;
            }
            return true;
        }

        int size() {
            validateAndRemoveKeysForExpiry();
            return map.size();
        }

        private long getCurrentTime() {
            return Instant.now().toEpochMilli();
        }

        private void validateAndRemoveKeysForExpiry() {
            long current = getCurrentTime();
            Iterator<Map.Entry<CacheKey, CacheEntry<Object>>> it = map.entrySet().iterator();

            while (it.hasNext()) {
                var entry = it.next();

                if (entry.getValue().isExpired(current)) {
                    evictionPolicy.onRemove(entry.getKey());
                    it.remove();
                }
            }
        }

    }
}
// 1:37