package com.distkv.common;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHasher {
    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final int virtualNodes;

    public ConsistentHasher(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    public void addWorker(String workerId) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(workerId + i);
            ring.put(hash, workerId);
        }
    }

    public void removeWorker(String workerId) {
        ring.values().removeIf(id -> id.equals(workerId));
    }

    public String getPrimary(String key) {
        if (ring.isEmpty()) return null;
        long hash = hash(key);
        SortedMap<Long, String> tailMap = ring.tailMap(hash);
        long targetHash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        return ring.get(targetHash);
    }

    public String[] getReplicas(String key, int count) {
        if (ring.isEmpty()) return new String[0];
        String[] replicas = new String[count];
        long hash = hash(key);
        
        SortedMap<Long, String> tailMap = ring.tailMap(hash);
        if (tailMap.isEmpty()) tailMap = ring;

        int found = 0;
        for (String workerId : tailMap.values()) {
            if (!contains(replicas, workerId)) {
                replicas[found++] = workerId;
                if (found == count) break;
            }
        }

        // If not enough unique workers in tailMap, wrap around
        if (found < count) {
            for (String workerId : ring.values()) {
                if (!contains(replicas, workerId)) {
                    replicas[found++] = workerId;
                    if (found == count) break;
                }
            }
        }
        return replicas;
    }

    private boolean contains(String[] array, String value) {
        if (value == null) return false;
        for (String s : array) {
            if (value.equals(s)) return true;
        }
        return false;
    }

    public static long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            // First 8 bytes to long
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
