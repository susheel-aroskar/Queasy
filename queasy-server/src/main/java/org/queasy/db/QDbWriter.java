package org.queasy.db;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultSetAccumulator;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author saroskar
 * Created on: 2021-04-01
 */
public class QDbWriter implements ResultSetAccumulator<Long> {

    private final Jdbi jdbi;
    private final String insertSQL;
    private final String autoGenIdColumnName;
    private final int insertBatchSize;

    private volatile long lastWrittenMessageId;

    private Handle handle;
    private PreparedBatch batch;
    private long batchTS;
    private int batchCount;

    private static final Logger logger = LoggerFactory.getLogger(QDbWriter.class);


    public QDbWriter(final Jdbi jdbi, final String qTable, final String autoGenIdColumnName, final int insertBatchSize) {
        this.jdbi = jdbi;
        this.insertSQL = String.format("INSERT INTO %s (qname, type, ts, mesg) VALUES (?, ?, ?, ?)", qTable);
        this.autoGenIdColumnName = autoGenIdColumnName;
        this.insertBatchSize = insertBatchSize;
    }

    public long getLastWrittenMessageId() {
        return lastWrittenMessageId;
    }

    private void startBatch() throws Exception {
        if (handle == null) {
            handle = jdbi.open();
        }
        handle.begin();
        batch = handle.prepareBatch(insertSQL);
        batchTS = System.currentTimeMillis();
    }

    public void batchWrite(final String[] message) throws Exception {
        if (batchCount == 0) {
            startBatch();
        }

        batch.bind(0, message[0])   //qname
                .bindNull(1, Types.VARCHAR) //type
                .bind(2, batchTS)
                .bind(3, message[1])
                .add();

        batchCount++;

        if (batchCount >= insertBatchSize) {
            finishBatch();
        }
    }

    private void finishBatch() {
        try {
            if (batchCount > 0) {
                final long newId = batch
                        .executeAndReturnGeneratedKeys("rowid")
                        .reduceResultSet(0L, this);
                handle.commit();
                lastWrittenMessageId = newId;
                batch.close();
            }
        } catch (Exception ex) {
            logger.error("Error finishing batch", ex);
        } finally {
            batch = null;
            batchCount = 0;
        }
    }

    private void closeHandle() {
        try {
            if (handle != null) {
                handle.close();
            }
        } catch (Exception ex) {
            logger.error("Error closing handle", ex);
        } finally {
            handle = null;
        }
    }

    public void finish() {
        finishBatch();
        closeHandle();
    }

    @Override
    public Long apply(final Long previous, final ResultSet rs, final StatementContext ctx) throws SQLException {
        final Long id = rs.getLong(autoGenIdColumnName);
        return id.compareTo(previous) > 0 ? id : previous;
    }

}
