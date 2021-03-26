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

    private WebSocketPolicy configureWebSocketPolicy(final WebSocketConfiguration wsConfig, final WebSocketPolicy policy) {
        policy.setAsyncWriteTimeout(wsConfig.getAsyncWriteTimeout());
        policy.setIdleTimeout(wsConfig.getIdleTimeout());
        policy.setInputBufferSize(wsConfig.getInputBufferSize());
        policy.setMaxTextMessageBufferSize(wsConfig.getMaxTextMessageBufferSize());
        policy.setMaxTextMessageSize(wsConfig.getMaxTextMessageSize());
        policy.setMaxBinaryMessageBufferSize(wsConfig.getMaxBinaryMessageBufferSize());
        policy.setMaxBinaryMessageSize(wsConfig.getMaxBinaryMessageSize());
        return policy;
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

        final QueueWriter queueWriter = new QueueWriter(qConfig, jdbi);
        env.lifecycle().manage(queueWriter);

        final ServletContextHandler servletCtxHandler = env.getApplicationContext();

        NativeWebSocketServletContainerInitializer.configure(servletCtxHandler, ((servletContext, nativeWebSocketConfiguration) -> {
            // Setup queueWriter websocket handler
            final WebSocketConfiguration wsConfig = config.getWebSocketConfiguration();
            configureWebSocketPolicy(wsConfig, nativeWebSocketConfiguration.getPolicy());
            nativeWebSocketConfiguration.addMapping("/nq/*", new ProducerWebSocketCreator(wsConfig.getOrigin(),
                    config.getMaxConnections(), queueWriter));

            //Thread pool to handle consumer groups
            final ScheduledExecutorService cgPool = env.lifecycle()
                    .scheduledExecutorService("cg-dispatcher-")
                    .threads(config.getConsumerGroupsThreadPoolSize())
                    .shutdownTime(config.getShutdownGracePeriod())
                    .build();

            // Set up consumer groups WebSocket handlers
            final long pollInterval = config.getNewMessagePollInterval().toMilliseconds();
            final Map<String, ConsumerGroupConfiguration> consumerConfigs = config.getConsumerGroups();
            for (Map.Entry<String, ConsumerGroupConfiguration> consumerCfg : consumerConfigs.entrySet()) {
                final ConsumerGroup consumerGroup = new ConsumerGroup(consumerCfg.getKey(), consumerCfg.getValue(),
                        qConfig, jdbi, queueWriter);
                nativeWebSocketConfiguration.addMapping("/dq/" + consumerGroup.getConsumerGroupName(),
                        new ConsumerGroupWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), consumerGroup));
                cgPool.scheduleAtFixedRate(consumerGroup, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
            }
        }));

        WebSocketUpgradeFilter.configure(servletCtxHandler);
    }

}
