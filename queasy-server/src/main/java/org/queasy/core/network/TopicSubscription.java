package org.queasy.core.network;

import org.queasy.core.managed.Topic;

import java.util.List;

/**
 * @author saroskar
 * Created on: 2021-04-05
 */
public final class TopicSubscription extends BaseWebSocketConnection {

    private final Topic topic;

    private volatile List<String> messages;
    private volatile long messageBatchId;
    private volatile int currentMesgIndex;

    public TopicSubscription(Topic topic) {
        this.topic = topic;
    }

    public long getMessageBatchId() {
        return messageBatchId;
    }

    @Override
    protected void onConnect() {
        topic.subscribe(this);
    }

    @Override
    protected void onDisconnect() {
        topic.unsubscribe(this);
    }

    public void setNextMessageBatch(final long nextBatchId, final List<String> nextMesgs) {
        if (nextBatchId > messageBatchId +1) {
            // Missed one or more message batches
            sendStatus(Status.MESG_DROP);
        }

        if (messageBatchId < nextBatchId) {
            messageBatchId = nextBatchId;
            messages = nextMesgs;
            currentMesgIndex = 0;
        }

        sendNextMessage();
    }

    public void sendNextMessage() {
        if ((messages != null) && (currentMesgIndex < messages.size())) {
            final String message = messages.get(currentMesgIndex);
            currentMesgIndex += 1;
            writeMessage(message);
        } else {
            // sent all messages, ask for more
            messages = null; // Friends of GC
            topic.waitForMessages(this);
        }
    }

    @Override
    public void writeSuccess() {
        // last message send was successful, send next one
        sendNextMessage();
    }

}
