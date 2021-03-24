package org.queasy;

import io.dropwizard.ConfiguredBundle;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ReadOnlyAppStateBundle implements ConfiguredBundle<ServerConfiguration> {

    @Override
    public void run(final ServerConfiguration configuration, final Environment environment) throws Exception {
        final String qName = configuration.getQueue().getName();
        final JdbiFactory jdbiFactory = new JdbiFactory();
        final Jdbi jdbi = jdbiFactory.build(environment, configuration.getDatabase(), qName);

        //Infer user defined message field mappings from the queue table in the database
        final Map<String, Integer> fieldMappings = jdbi.withHandle( handle -> {
            final Query query = handle.createQuery("select * from " + qName + " limit 1");
            return query.scanResultSet(this::scanResultSet);
        });

        configuration.setReadOnlyAppState(new ReadOnlyAppState(jdbi, fieldMappings));
    }

    private Map<String, Integer> scanResultSet(final Supplier<ResultSet> resultSetSupplier, final StatementContext ctx) throws SQLException {
        final ResultSet rs = resultSetSupplier.get();
        final ResultSetMetaData metaData = rs.getMetaData();
        final Map<String, Integer> fieldMappings = new LinkedHashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            fieldMappings.put(metaData.getColumnName(i), metaData.getColumnType(i));
        }
        return fieldMappings;
    }

}
