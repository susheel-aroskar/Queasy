package org.queasy.core.config;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public class TopicConfiguration extends ConsumerGroupConfiguration {

    /**
     * If topic waits for every single subscriber to finish consuming all the messages from the current message batch,
     * a single slow subscriber will could  back all subscribers for consuming newer messages from the next batch.
     * Instead, topic waits till a configured quorum percentage of consumers have finished consuming the current message
     * batch. Quorum percentage is calculated as (number of subscribers done with current batch / total number of
     * subscribers subscribed to this topic) * 100
     */
    @Min(1)
    @Max(100)
    private int quorumPercentage = 50;

    public int getQuorumPercentage() {
        return quorumPercentage;
    }

    public void setQuorumPercentage(int quorumPercentage) {
        this.quorumPercentage = quorumPercentage;
    }

}
