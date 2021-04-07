package org.queasy;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.queasy.core.bundles.QueasyServerBundle;
import org.queasy.core.bundles.QueasyMigrationBundle;

import javax.servlet.ServletException;

public class ServerApplication extends Application<ServerConfiguration> {

    public static void main(final String[] args) throws Exception {
        new ServerApplication().run(args);
    }

    @Override
    public String getName() {
        return "QueasyServer";
    }

    @Override
    public void initialize(final Bootstrap<ServerConfiguration> bootstrap) {
        bootstrap.addBundle(new QueasyMigrationBundle());
        bootstrap.addBundle(new QueasyServerBundle());
    }

    @Override
    public void run(final ServerConfiguration config, final Environment env) throws ServletException {
    }

}
