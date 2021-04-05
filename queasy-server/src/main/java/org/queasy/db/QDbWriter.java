package org.queasy.db;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.util.Snowflake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

/**
 * @author saroskar
 * Created on: 2021-04-01
 */
public class QDbWriter {

    private final Snowflake idGenerator;
    private final Jdbi jdbi;
    private final String insertSQL;
    private final int insertBatchSize;

    private volatile long lastWrittenMessageId;
    private long currentId;

    private Handle handle;
    private PreparedBatch batch;
    private long batchTS;
    private int batchCount;

    private static final Logger logger = LoggerFactory.getLogger(QDbWriter.class);


    public QDbWriter(final Snowflake idGenerator, final Jdbi jdbi, final WriterConfiguration writerConfig) {
        this.idGenerator = idGenerator;
        this.jdbi = jdbi;
        this.insertSQL = String.format("INSERT INTO %s (id, qname, type, ts, mesg) VALUES (?, ?, ?, ?, ?)",
                writerConfig.getTableName());
        this.insertBatchSize = writerConfig.getInsertBatchSize();
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

        currentId = idGenerator.nextId();
        batch.bind(0, currentId) //id
                .bind(1, message[0])   //qname
                .bindNull(2, Types.VARCHAR) //type
                .bind(3, batchTS) //timestamp
                .bind(4, message[1]) //message
                .add();

        batchCount++;

        if (batchCount >= insertBatchSize) {
            finishBatch();
        }
    }

    private void finishBatch() {
        try {
            if (batchCount > 0) {
                batch.execute();
                handle.commit();
                lastWrittenMessageId = currentId;
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

}
