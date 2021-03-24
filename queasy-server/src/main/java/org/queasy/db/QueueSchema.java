package org.queasy.db;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.StatementContext;
import org.queasy.core.config.QueueConfiguration;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * @author saroskar
 * Created on: 2021-03-23
 */
public class QueueSchema {

    private final Jdbi jdbi;
    private final String qName;
    private final List<String> allColumns;
    private final List<String> userDefinedColumns;
    private final String insertStmt;

    public QueueSchema(Jdbi jdbi, QueueConfiguration qConfig) {
        this.jdbi = jdbi;
        qName = qConfig.getName();
        final ArrayList<String> columns = findColummns(jdbi, qName);
        allColumns = ImmutableList.copyOf(columns);
        userDefinedColumns = ImmutableList.copyOf(columns.subList(4, columns.size()));
        insertStmt = buildInsertStatement(qName, allColumns);
    }

    private static ArrayList<String> findColummns(final Jdbi jdbi, final String qName) {
        //Infer user defined message field mappings from the queue table in the database
        return jdbi.withHandle(handle -> {
            final Query query = handle.createQuery("select * from " + qName + " limit 1");
            return query.scanResultSet(QueueSchema::scanResultSet);
        });
    }

    private static ArrayList<String> scanResultSet(final Supplier<ResultSet> resultSetSupplier, final StatementContext ctx) throws SQLException {
        final ResultSet rs = resultSetSupplier.get();
        final ResultSetMetaData metaData = rs.getMetaData();
        final ArrayList<String> columns = new ArrayList<>(16);
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.add(metaData.getColumnName(i));
        }
        return columns;
    }

    private static String buildInsertStatement(final String qName, final List<String> columns) {
        final Joiner joiner = Joiner.on(", ");
        final StringBuilder s = new StringBuilder("INSERT INTO ").append(qName)
                .append(" (").append(joiner.join(columns)).append(")")
                .append(" VALUES")
                .append(" (")
                .append(joiner.join(Collections.nCopies(columns.size(), "?")))
                .append(")");
        return s.toString();
    }

    public List<String> getUserDefinedColumns() {
        return userDefinedColumns;
    }

    public String getInsertStmt() {
        return insertStmt;
    }

    public String builSelectQuery(final String condition) {
        return Strings.isNullOrEmpty(condition) ?
                String.format("SELECT * FROM %s WHERE id > ? AND id <= ? LIMIT ?", qName) :
                String.format("SELECT * FROM %s WHERE id > ? AND id <= ? AND %s LIMIT ?", qName, condition);
    }

    public Jdbi getJdbi() {
        return jdbi;
    }

    public String getqName() {
        return qName;
    }

    public List<String> getAllColumns() {
        return allColumns;
    }
}
