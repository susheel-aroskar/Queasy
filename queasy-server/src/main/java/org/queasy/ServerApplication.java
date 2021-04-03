package org.queasy;

import io.dropwizard.Application;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.server.NativeWebSocketServletContainerInitializer;
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter;
import org.jdbi.v3.core.Jdbi;
import org.queasy.core.bundles.QueasyMigrationBundle;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.config.WebSocketConfiguration;
import org.queasy.core.managed.ConsumerGroup;
import org.queasy.core.managed.QueueWriter;
import org.queasy.core.network.ConsumerGroupWebSocketCreator;
import org.queasy.core.network.ProducerWebSocketCreator;
import org.queasy.core.util.Snowflake;
import org.queasy.db.QDbReader;
import org.queasy.db.QDbWriter;

import javax.servlet.ServletException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    }

    @Override
    public void run(final ServerConfiguration config, final Environment env) throws ServletException {
        final QueueConfiguration qConfig = config.getQueue();

        final JdbiFactory jdbiFactory = new JdbiFactory();
        final Jdbi jdbi = jdbiFactory.build(env, config.getDatabase(), qConfig.getTableName());

        final Snowflake idGenerator = new Snowflake(config.getHostId());

        final QDbWriter qDbWriter = new QDbWriter(idGenerator, jdbi, qConfig);

        final QueueWriter queueWriter = new QueueWriter(qConfig, qDbWriter);
        env.lifecycle().manage(queueWriter);

        //Thread pool to handle consumer groups
        final ScheduledExecutorService cgPool = env.lifecycle()
                .scheduledExecutorService("cg-dispatcher-")
                .threads(config.getConsumerGroupsThreadPoolSize())
                .shutdownTime(config.getShutdownGracePeriod())
                .build();

        final long pollInterval = config.getNewMessagePollInterval().toMilliseconds();
        final WebSocketConfiguration wsConfig = config.getWebSocketConfiguration();
        final Map<String, ConsumerGroupConfiguration> consumerConfigs = config.getConsumerGroups();
        final ServletContextHandler servletCtxHandler = env.getApplicationContext();

        NativeWebSocketServletContainerInitializer.configure(servletCtxHandler, ((servletContext, nativeWebSocketConfiguration) -> {
            // Set up queueWriter websocket handler
            wsConfig.configureWebSocketPolicy(nativeWebSocketConfiguration.getPolicy());
            nativeWebSocketConfiguration.addMapping("/nq/*", new ProducerWebSocketCreator(wsConfig.getOrigin(),
                    config.getMaxConnections(), queueWriter));

            // Set up consumer groups WebSocket handlers
            for (Map.Entry<String, ConsumerGroupConfiguration> consumerCfg : consumerConfigs.entrySet()) {
                final String consumerGroupName  = consumerCfg.getKey();
                final ConsumerGroupConfiguration cgConfig = consumerCfg.getValue();
                final QDbReader qDbReader = new QDbReader(qDbWriter, jdbi, qConfig, consumerGroupName, cgConfig);
                final ConsumerGroup consumerGroup = new ConsumerGroup(qDbReader);
                nativeWebSocketConfiguration.addMapping("/dq/" + consumerGroupName,
                        new ConsumerGroupWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), consumerGroup));
                env.lifecycle().manage(consumerGroup);
                cgPool.scheduleAtFixedRate(consumerGroup, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
            }
        }));

        WebSocketUpgradeFilter.configure(servletCtxHandler);
    }

}
