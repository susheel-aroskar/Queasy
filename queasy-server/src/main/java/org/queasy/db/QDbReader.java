package org.queasy.db;

import com.google.common.annotations.VisibleForTesting;
import org.jdbi.v3.core.Jdbi;
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
    private final String selectSQL;

    private volatile long lastReadMessageId;
    private volatile long lastCkptMessageId;
    private volatile long numOfDbFetches;


    private static final String SELECT_CHECKPOINT_SQL = "SELECT checkpoint FROM queasy_checkpoint WHERE cg_name = ?";
    private static final String INSERT_CHECKPOINT_SQL = "INSERT INTO queasy_checkpoint (cg_name, checkpoint, ts) " +
            "VALUES (?, ?, ?)";
    private static final String UPDATE_CHECKPOINT_SQL = "UPDATE queasy_checkpoint SET checkpoint = ?, ts = ?" +
            " where cg_name = ? ";

    private static final Logger logger = LoggerFactory.getLogger(QDbReader.class);


    public QDbReader(final QDbWriter qDbWriter, final Jdbi jdbi, final String ckptName,
                     final String qTable, final int fetchSize, final String query) {
        this.qDbWriter = qDbWriter;
        this.jdbi = jdbi;
        this.ckptName = ckptName;
        this.fetchSize = fetchSize;
        this.selectSQL = String.format("SELECT id, mesg FROM %s WHERE id > ? AND %s AND type is NULL", qTable, query);
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

    public int getFetchSize() {
        return fetchSize;
    }

    public boolean loadNextBatchOfMessages(final Collection<String> messages) {
        // Persist checkpoint only after all messages in the batch are dispatched to clients
        saveCheckpoint();

        if (qDbWriter.getLastWrittenMessageId() <= lastReadMessageId) {
            return false; // Writer hasn't advanced
        }

        numOfDbFetches++;
        final long lastWrittenMessageId = qDbWriter.getLastWrittenMessageId();
        final long oldLastReadMessageId = lastReadMessageId;
        jdbi.useHandle(handle ->
                handle.select(selectSQL, lastReadMessageId)
                        .setFetchSize(fetchSize)
                        .setMaxRows(fetchSize)
                        .map((rs, ctx) -> {
                            lastReadMessageId = rs.getLong(1);
                            return rs.getString(2);
                        })
                        .forEach(s -> {
                            if (!messages.add(s)) {
                                // Can never happen as long as ConsumerGroup sets messages size = fetchSize + 1
                                logger.error("ERROR! Could not add message read from DB to messages to deliver: " + selectSQL);
                            }
                        })
        );

        if (lastReadMessageId > oldLastReadMessageId) {
            // New messages found
            return true;
        } else {
            // This can happen if writer has inserted new messages but none of them match the "query" for this consumer
            // group. In such cases we want to advance lastReadMessageId - and the checkpoint -to lastWrittenMessageId
            // because we want poll messages from that point next time onwards
            lastReadMessageId = lastWrittenMessageId;
            return false;
        }
    }


    @VisibleForTesting
    public long getLastReadMessageId() {
        return lastReadMessageId;
    }

    @VisibleForTesting
    public long getNumOfDbFetches() {
        return numOfDbFetches;
    }
}
