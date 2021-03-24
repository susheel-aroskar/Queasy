package org.queasy.core.mesg;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public abstract class CheckpointMessage implements Message {

    @Override
    public MessageType getType() {
        return MessageType.CHECKPOINT;
    }
}
