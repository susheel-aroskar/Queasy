package org.queasy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.dropwizard.Application;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.server.NativeWebSocketServletContainerInitializer;
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter;
import org.jdbi.v3.core.Jdbi;
import org.queasy.core.bundles.QueasyMigrationBundle;
import org.queasy.core.config.CacheConfiguration;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.WriterConfiguration;
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

    public static final String PUBLISH_PATH = "publish";
    public static final String DEQUEUE_PATH = "dequeue";

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
        final WriterConfiguration writerConfig = config.getWriterConfiguration();

        final JdbiFactory jdbiFactory = new JdbiFactory();
        final Jdbi jdbi = jdbiFactory.build(env, config.getDatabase(), writerConfig.getTableName());

        final Snowflake idGenerator = new Snowflake(config.getHostId());

        final QDbWriter qDbWriter = new QDbWriter(idGenerator, jdbi, writerConfig);

        final QueueWriter queueWriter = new QueueWriter(writerConfig, qDbWriter);
        env.lifecycle().manage(queueWriter);

        final Cache<Long, String> messageCache = buildMessagesCache(config.getCacheConfiguration());

        //Thread pool to handle consumer groups
        final ScheduledExecutorService cgPool = env.lifecycle()
                .scheduledExecutorService("cg-dispatcher-")
                .threads(config.getConsumerGroupsThreadPoolSize())
                .shutdownTime(config.getShutdownGracePeriod())
                .build();

        final long pollInterval = config.getNewMessagePollInterval().toMilliseconds();
        final WebSocketConfiguration wsConfig = config.getWebSocketConfiguration();
        final Map<String, ConsumerGroupConfiguration> cgConfigs = config.getConsumerGroups();
        final ServletContextHandler servletCtxHandler = env.getApplicationContext();

        NativeWebSocketServletContainerInitializer.configure(servletCtxHandler, ((servletContext, nativeWebSocketConfiguration) -> {
            // Set up queueWriter websocket handler
            wsConfig.configureWebSocketPolicy(nativeWebSocketConfiguration.getPolicy());
            nativeWebSocketConfiguration.addMapping(String.format("/%s/*",PUBLISH_PATH), new ProducerWebSocketCreator(wsConfig.getOrigin(),
                    config.getMaxConnections(), queueWriter));

            // Set up consumer groups WebSocket handlers
            for (Map.Entry<String, ConsumerGroupConfiguration> cg : cgConfigs.entrySet()) {
                final String cgName  = cg.getKey();
                final ConsumerGroupConfiguration cgConfig = cg.getValue();
                final QDbReader qDbReader = new QDbReader(qDbWriter, jdbi, writerConfig, cgName, cgConfig, messageCache);
                final ConsumerGroup consumerGroup = new ConsumerGroup(qDbReader);
                nativeWebSocketConfiguration.addMapping(String.format("/%s/%s/*", DEQUEUE_PATH, cgName),
                        new ConsumerGroupWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), consumerGroup));
                env.lifecycle().manage(consumerGroup);
                cgPool.scheduleAtFixedRate(consumerGroup, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
            }
        }));

        WebSocketUpgradeFilter.configure(servletCtxHandler);
    }

    private Cache<Long, String> buildMessagesCache(final CacheConfiguration cacheConfig) {
        return (cacheConfig.isEnabled()) ?
                Caffeine.newBuilder()
                        .initialCapacity(cacheConfig.getInitialCapacity())
                        .maximumSize(cacheConfig.getMaxSize())
                        .expireAfterWrite(cacheConfig.getExpireAfter().toMilliseconds(), TimeUnit.MILLISECONDS)
                        .build() : null;
    }

}
