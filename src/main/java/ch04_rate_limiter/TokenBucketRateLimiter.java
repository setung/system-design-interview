package ch04_rate_limiter;

import java.util.concurrent.atomic.AtomicInteger;


public class TokenBucketRateLimiter {

    private final int maxTokens; // 최대 토큰 수
    private final int refillRate; // 초당 리필되는 토큰 수
    private final long refillIntervalInMillis; // 리필 간격 (밀리초 단위)
    private final AtomicInteger currentTokens; // 현재 토큰 수
    private volatile long lastRefillTimestamp; // 마지막 리필 시점

    public TokenBucketRateLimiter(int maxTokens, int refillRate, long refillIntervalInMillis) {
        this.maxTokens = maxTokens;
        this.refillRate = refillRate;
        this.refillIntervalInMillis = refillIntervalInMillis;
        this.currentTokens = new AtomicInteger(maxTokens);
        this.lastRefillTimestamp = System.currentTimeMillis();
    }

    // 요청 시 토큰 리필 및 소비
    public  boolean tryAcquire() {
        refillTokensIfNeeded();

        if (currentTokens.get() > 0) {
            currentTokens.decrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    // 필요한 경우 토큰 리필
    private void refillTokensIfNeeded() {
        long now = System.currentTimeMillis();
        long elapsedTime = now - lastRefillTimestamp;

        if (elapsedTime > refillIntervalInMillis) {
            int tokensToAdd = (int) (elapsedTime / refillIntervalInMillis * refillRate);
            int newTokenCount = Math.min(maxTokens, currentTokens.get() + tokensToAdd);
            currentTokens.set(newTokenCount); // 토큰 갱신
            lastRefillTimestamp = now; // 리필 시점 갱신
        }
    }

    public static void main(String[] args) throws InterruptedException {

        // 최대 5개의 토큰, 초당 2개 리필
        TokenBucketRateLimiter rateLimiter = new TokenBucketRateLimiter(5, 2, 1000);

        // 테스트 시뮬레이션
        Runnable requestTask = () -> {
            if (rateLimiter.tryAcquire()) {
                System.out.println("Request processed at " + System.currentTimeMillis());
            } else {
                System.out.println("Request denied at " + System.currentTimeMillis());
            }
        };

        // 200ms 간격으로 요청
        for (int i = 0; i < 50; i++) {
            Thread thread = new Thread(requestTask);
            thread.start();
            Thread.sleep(200);
        }
    }
}
