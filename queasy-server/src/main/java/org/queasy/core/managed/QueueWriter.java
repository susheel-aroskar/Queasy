package org.queasy.core.managed;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.db.QDbWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public final class QueueWriter implements Managed, Runnable {

    private QDbWriter qDbWriter;
    private final ArrayBlockingQueue<String[]> queue;
    private final long writeTimeout;
    private Thread writerThread;

    private volatile boolean shutdownFlag;

    private static final Logger logger = LoggerFactory.getLogger(QueueWriter.class);


    public QueueWriter(final WriterConfiguration writerConfig, final QDbWriter qDbWriter) {
        this.qDbWriter = qDbWriter;
        queue = new ArrayBlockingQueue<>(writerConfig.getRingBufferSize());
        writeTimeout = writerConfig.getWriteTimeout().toMilliseconds();
    }

    @Override
    public void start() {
        shutdownFlag = false;
        writerThread = new Thread(this, "queasy-writer");
        writerThread.start();
    }

    @Override
    public void stop() {
        shutdownFlag = true;
        writerThread.interrupt();
    }

    public boolean publish(final String[] message) throws InterruptedException {
        return queue.offer(message, writeTimeout, TimeUnit.MILLISECONDS);
    }

    public void run() {
        while (!shutdownFlag) {
            try {
                String[] message = queue.take();
                while (message != null) {
                    qDbWriter.batchWrite(message);
                    message = queue.poll();
                }
            }
            catch (Exception ex) {
                logger.error("Exception in Queue writer: ", ex);
            }
            finally {
                qDbWriter.finish();
            }
        }
    }


    @VisibleForTesting
    public void join() throws InterruptedException {
        writerThread.join();
    }

    @VisibleForTesting
    public void drainQueue() {
        queue.clear();
    }

}
