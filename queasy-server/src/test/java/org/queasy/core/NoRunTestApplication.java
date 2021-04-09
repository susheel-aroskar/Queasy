package org.queasy.core;

import io.dropwizard.setup.Environment;
import org.queasy.ServerApplication;
import org.queasy.ServerConfiguration;

import javax.servlet.ServletException;

/**
 * @author saroskar
 * Created on: 2021-03-30
 */
public class NoRunTestApplication extends ServerApplication {
    @Override
    public void run(ServerConfiguration config, Environment env) {
        // Don't run machinery
    }
}
