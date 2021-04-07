package org.queasy.db;

import com.github.benmanes.caffeine.cache.Cache;
import org.jdbi.v3.core.Jdbi;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.WriterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

/**
 * @author saroskar
 * Created on: 2021-04-01
 */
public class QDbReader {

    private final QDbWriter qDbWriter;
    private final Jdbi jdbi;
    private final String ckptName;
    private final int fetchSize;
    private final long timeout;
    private final String selectSQL;
    private final Cache<Long, String> messageCache;

    private volatile long lastReadMessageId;
    private volatile long lastCkptMessageId;
    private volatile long readBatchId;


    private static final String SELECT_CHECKPOINT_SQL = "SELECT checkpoint FROM queasy_checkpoint WHERE cg_name = ?";
    private static final String INSERT_CHECKPOINT_SQL = "INSERT INTO queasy_checkpoint (cg_name, checkpoint, ts) " +
            "VALUES (?, ?, ?)";
    private static final String UPDATE_CHECKPOINT_SQL = "UPDATE queasy_checkpoint SET checkpoint = ?, ts = ?" +
            " where cg_name = ? ";

    private static final Logger logger = LoggerFactory.getLogger(QDbReader.class);


    public QDbReader(final QDbWriter qDbWriter, final Jdbi jdbi, final WriterConfiguration writerConfig,
                     final String cgName, final ConsumerGroupConfiguration cgConfig, final Cache<Long, String> cache) {
        this.qDbWriter = qDbWriter;
        this.jdbi = jdbi;
        this.ckptName = cgName;
        this.fetchSize = cgConfig.getFetchBatchSize();
        this.timeout = cgConfig.getTimeOut().toMilliseconds();
        this.selectSQL = String.format("SELECT id, mesg FROM %s WHERE id > ? AND %s AND type is NULL",
                writerConfig.getTableName(), cgConfig.getQuery());
        this.messageCache = cache;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getLastReadMessageId() {
        return lastReadMessageId;
    }

    /**
     * Tracks DB fetches, that is, groups of messages read as a batch. Is monotonically increasing so that it can be
     * used to sense missed message batches in case of a slow pub-sub or topic consumer
     *
     * @return
     */
    public long getReadBatchId() {
        return readBatchId;
    }

    public int getFetchSize() {
        return fetchSize;
    }


    public long readLastCheckpoint() {
        final Optional<Long> result = jdbi.withHandle(handle -> handle.select(SELECT_CHECKPOINT_SQL, ckptName)
                .map((rs, col, ctx) -> rs.getLong(col))
                .findOne());

        if (result.isPresent()) {
            lastReadMessageId = result.get();
        } else {
            //No checkpoint established. Use producer's currentId as default and checkpoint it to the DB
            lastReadMessageId = qDbWriter.getLastWrittenMessageId();
            jdbi.withHandle(handle ->
                    handle.execute(INSERT_CHECKPOINT_SQL, ckptName, lastReadMessageId, System.currentTimeMillis()));
        }

        return lastReadMessageId;
    }

    public void saveCheckpoint() {
        if (lastReadMessageId > lastCkptMessageId) {
            jdbi.useHandle(handle -> handle.execute(UPDATE_CHECKPOINT_SQL,
                    lastReadMessageId, System.currentTimeMillis(), ckptName));
            lastCkptMessageId = lastReadMessageId;
        }
    }

    public boolean hasMoreMessages() {
        return (qDbWriter.getLastWrittenMessageId() > lastReadMessageId);
    }

    public boolean loadNextBatchOfMessages(final Collection<String> messages) {
        // Persist checkpoint only after all messages in the batch are dispatched to clients
        saveCheckpoint();

        if (!hasMoreMessages()) {
            return false; // Writer hasn't advanced
        }

        final long lastWrittenMessageId = qDbWriter.getLastWrittenMessageId();
        final long oldLastReadMessageId = lastReadMessageId;
        jdbi.useHandle(handle ->
                handle.select(selectSQL, lastReadMessageId)
                        .setFetchSize(fetchSize)
                        .setMaxRows(fetchSize)
                        .map((rs, ctx) -> {
                            lastReadMessageId = rs.getLong(1);
                            final String message = rs.getString(2);
                            return (messageCache != null) ?
                                    messageCache.get(lastReadMessageId, id -> buildMessage(id, message)) :
                                    buildMessage(lastReadMessageId ,message);
                        })
                        .forEach(s -> {
                            if (!messages.add(s)) {
                                // Can never really happen as long as ConsumerGroup sets messages size = fetchSize + 1
                                logger.error("ERROR! Could not add message read from DB to messages to deliver: " + selectSQL);
                            }
                        })
        );

        if (lastReadMessageId > oldLastReadMessageId) {
            // New messages found
            readBatchId++;
            return true;
        } else {
            // This can happen if writer inserts new messages but none of them match the "query" for this consumer
            // group. In such cases we do want to advance lastReadMessageId - and the checkpoint - to
            // lastWrittenMessageId because we want to poll messages from that point next time onwards
            lastReadMessageId = lastWrittenMessageId;
            saveCheckpoint();
            return false;
        }
    }

    public static String buildMessage(final long id, final String message) {
        return String.format("{\"id\": %s, \"message\": %s}", id, message);
    }

}
