package org.queasy.core.config;

import io.dropwizard.util.Duration;

import javax.validation.constraints.NotNull;

/**
 * Configuration of the JVM-wide global Caffeine cache used to share message instances across different consumer
 * group in a manner similar to String.intern() but without permanent pinning them to PermGen like String.intern()
 *
 * @author saroskar
 * Created on: 2021-04-02
 */
public class CacheConfiguration {

    private boolean enabled = true;

    @NotNull
    private int initialCapacity = 1024 * 4;

    @NotNull
    private int maxSize = 1024 * 8;

    @NotNull
    private Duration expireAfter = Duration.minutes(8);


    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getInitialCapacity() {
        return initialCapacity;
    }

    public void setInitialCapacity(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public Duration getExpireAfter() {
        return expireAfter;
    }

    public void setExpireAfter(Duration expireAfter) {
        this.expireAfter = expireAfter;
    }
}
