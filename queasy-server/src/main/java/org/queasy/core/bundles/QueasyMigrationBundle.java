package org.queasy.core.bundles;

import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.migrations.MigrationsBundle;
import org.queasy.ServerConfiguration;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class QueasyMigrationBundle extends MigrationsBundle<ServerConfiguration> {

    @Override
    public PooledDataSourceFactory getDataSourceFactory(ServerConfiguration serverConfiguration) {
        return serverConfiguration.getDatabase();
    }
    @Override
    public String getMigrationsFileName() {
        return "queue_schema.yml";
    }

}
