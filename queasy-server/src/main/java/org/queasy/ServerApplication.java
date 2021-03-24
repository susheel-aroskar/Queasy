package org.queasy;

import io.dropwizard.Application;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
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
import org.queasy.core.managed.Producer;
import org.queasy.core.networking.ConsumerGroupWebSocketCreator;
import org.queasy.core.networking.ProducerWebSocketCreator;
import org.queasy.db.QueueSchema;

import javax.servlet.ServletException;
import java.util.Map;

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
        final Jdbi jdbi = jdbiFactory.build(env, config.getDatabase(), qConfig.getName());
        final QueueSchema queueSchema = new QueueSchema(jdbi, qConfig);

        final Producer producer = new Producer(qConfig, queueSchema);
        env.lifecycle().manage(producer);

        final ServletContextHandler servletCtxHandler = env.getApplicationContext();

        NativeWebSocketServletContainerInitializer.configure(servletCtxHandler, ((servletContext, nativeWebSocketConfiguration) -> {
            final WebSocketConfiguration wsConfig = config.getWebSocketConfiguration();
            configureWebSocketPolicy(wsConfig, nativeWebSocketConfiguration.getPolicy());
            nativeWebSocketConfiguration.addMapping("/publish", new ProducerWebSocketCreator(wsConfig, producer));

            final Map<String, ConsumerGroupConfiguration> consumerConfigs = config.getConsumerGroups();
            for(Map.Entry<String, ConsumerGroupConfiguration> consumerCfg : consumerConfigs.entrySet()) {
                final ConsumerGroup  consumerGroup = new ConsumerGroup(consumerCfg.getKey(), consumerCfg.getValue(), queueSchema, producer);
                env.lifecycle().manage(consumerGroup);
                nativeWebSocketConfiguration.addMapping("/"+consumerGroup.getName()+"/deque",
                        new ConsumerGroupWebSocketCreator(wsConfig, consumerGroup));
            }
        }));

        WebSocketUpgradeFilter.configure(servletCtxHandler);
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

}
