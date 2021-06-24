package redis.clients.jedis;

import java.util.concurrent.TimeUnit;

public class CacheConfig {

    public int maxSize;
    public int expireAfterAccess;
    public int expireAfterWrite;
    public TimeUnit timeUnit;

    public CacheConfig(Builder builder){
        this.maxSize = builder.maxSize;
        this.expireAfterAccess = builder.expireAfterAccess;
        this.expireAfterWrite = builder.expireAfterWrite;
        this.timeUnit = builder.timeUnit;
    }
    public int getMaxSize() {return maxSize;}
    public int getExpireAfterAccess() {return expireAfterAccess;}
    public int getExpireAfterWrite() {return expireAfterWrite;}
    public TimeUnit getUnit() { return  timeUnit;}
    public static class Builder {
        private int maxSize = 100;
        private int expireAfterAccess = 5;
        private int expireAfterWrite = 5;
        private TimeUnit timeUnit = TimeUnit.MINUTES;

        public static Builder newInstance() {
            return new Builder();
        }

        private Builder() {
        }

        // Setter methods
        public Builder maxSize(int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder expireAfterAccess(int expireAfterAccess) {
            this.expireAfterAccess = expireAfterAccess;
            return this;
        }

        public Builder expireAfterWrite(int expireAfterWrite) {
            this.expireAfterWrite = expireAfterWrite;
            return this;
        }

        public Builder timeUnit(TimeUnit timeUnit){
            this.timeUnit = timeUnit;
            return this;
        }

        public CacheConfig build() {
            return new CacheConfig(this);
        }
    }

}
