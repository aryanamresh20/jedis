package redis.clients.jedis;

import java.util.concurrent.TimeUnit;

public class JedisCacheConfig {

    private static final long MAX_CACHE_SIZE = 1000L;
    private static final long EXPIRE_AFTER_ACCESS_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final long EXPIRE_AFTER_WRITE_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private final long maxCacheSize;
    private final long expireAfterAccessMillis;
    private final long expireAfterWriteMillis;
    private final TrackingMode trackingMode;
    private final boolean optInCaching;
    private final boolean noLoop;

    public JedisCacheConfig(Builder builder) {
        this.maxCacheSize = builder.maxCacheSize;
        this.expireAfterAccessMillis = builder.expireAfterAccessMillis;
        this.expireAfterWriteMillis = builder.expireAfterWriteMillis;
        this.trackingMode = builder.trackingMode;
        this.optInCaching = builder.optInCaching;
        this.noLoop = builder.noLoop;
    }

    public long getMaxCacheSize() {
        return maxCacheSize;
    }

    public long getExpireAfterAccessMillis() {
        return expireAfterAccessMillis;
    }

    public long getExpireAfterWriteMillis() {
        return expireAfterWriteMillis;
    }

    public TrackingMode getTrackingMode() {
        return trackingMode;
    }

    public boolean isOptInCaching() {
        return optInCaching;
    }

    public boolean isNoLoop() {
        return noLoop;
    }

    public static class Builder {
        private long maxCacheSize = MAX_CACHE_SIZE;
        private long expireAfterAccessMillis = EXPIRE_AFTER_ACCESS_MILLIS;
        private long expireAfterWriteMillis = EXPIRE_AFTER_WRITE_MILLIS;
        private TrackingMode trackingMode = TrackingMode.DEFAULT;
        private boolean optInCaching;
        private boolean noLoop;

        public static Builder newBuilder() {
            return new Builder();
        }

        private Builder() {
        }

        // Setter methods
        public Builder maxCacheSize(long maxCacheSize) {
            this.maxCacheSize = maxCacheSize;
            return this;
        }

        public Builder expireAfterAccess(long expireAfterAccessMillis) {
            this.expireAfterAccessMillis = expireAfterAccessMillis;
            return this;
        }

        public Builder expireAfterWrite(long expireAfterWriteMillis) {
            this.expireAfterWriteMillis = expireAfterWriteMillis;
            return this;
        }

        public Builder trackingMode(TrackingMode trackingMode) {
            this.trackingMode = trackingMode;
            return this;
        }

        public Builder optInCaching(boolean optInCaching) {
            this.optInCaching = optInCaching;
            return this;
        }

        public Builder noLoop(boolean noLoop) {
            this.noLoop = noLoop;
            return this;
        }

        public JedisCacheConfig build() {
            return new JedisCacheConfig(this);
        }
    }

    public enum TrackingMode {
        DEFAULT,
        BROADCASTING
    }
}
