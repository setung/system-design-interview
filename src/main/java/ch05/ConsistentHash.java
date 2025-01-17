package ch05;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash {
    private final SortedMap<Integer, String> circle = new TreeMap<>();
    private final int numberOfReplicas;

    public ConsistentHash(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    // 노드를 해시 링에 추가
    public void addNode(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = getHash(node + i);
            circle.put(hash, node);
        }
    }

    // 노드를 해시 링에서 제거
    public void removeNode(String node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = getHash(node + i);
            circle.remove(hash);
        }
    }

    // 데이터 키에 해당하는 노드 찾기
    public String getNode(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = getHash(key);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    // SHA-256 기반 해시 함수
    private int getHash(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            return Math.abs(hashBytes[0] << 24 | (hashBytes[1] & 0xFF) << 16 | (hashBytes[2] & 0xFF) << 8 | (hashBytes[3] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    public static void main(String[] args) {
        ConsistentHash consistentHash = new ConsistentHash(3); // 가상 노드 100개

        // 노드 추가
        consistentHash.addNode("NodeA");
        consistentHash.addNode("NodeB");
        consistentHash.addNode("NodeC");

        // 데이터 키 할당 확인
        System.out.println("Key1 -> " + consistentHash.getNode("Key1"));
        System.out.println("Key2 -> " + consistentHash.getNode("Key2"));
        System.out.println("Key3 -> " + consistentHash.getNode("Key3"));

        // 노드 추가 후 데이터 이동 확인
        consistentHash.addNode("NodeD");
        System.out.println("\nAfter adding NodeD:");
        System.out.println("Key1 -> " + consistentHash.getNode("Key1"));
        System.out.println("Key2 -> " + consistentHash.getNode("Key2"));
        System.out.println("Key3 -> " + consistentHash.getNode("Key3"));

        // 노드 제거 후 데이터 이동 확인
        consistentHash.removeNode("NodeC");
        System.out.println("\nAfter removing NodeC:");
        System.out.println("Key1 -> " + consistentHash.getNode("Key1"));
        System.out.println("Key2 -> " + consistentHash.getNode("Key2"));
        System.out.println("Key3 -> " + consistentHash.getNode("Key3"));
    }
}