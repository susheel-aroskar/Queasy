package org.queasy.core.bundles;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.server.NativeWebSocketConfiguration;
import org.eclipse.jetty.websocket.server.WebSocketUpgradeFilter;
import org.jdbi.v3.core.Jdbi;
import org.queasy.ServerConfiguration;
import org.queasy.core.config.CacheConfiguration;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.TopicConfiguration;
import org.queasy.core.config.WebSocketConfiguration;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.managed.ConsumerGroup;
import org.queasy.core.managed.QueueWriter;
import org.queasy.core.managed.Topic;
import org.queasy.core.network.ConsumerGroupWebSocketCreator;
import org.queasy.core.network.ProducerWebSocketCreator;
import org.queasy.core.network.TopicSubscriptionWebSocketCreator;
import org.queasy.core.util.Snowflake;
import org.queasy.db.QDbReader;
import org.queasy.db.QDbWriter;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author saroskar
 * Created on: 2021-04-07
 */
public class QueasyServerBundle implements ConfiguredBundle<ServerConfiguration> {

    public static final String PUBLISH_PATH = "publish";
    public static final String DEQUEUE_PATH = "dequeue";
    public static final String SUBSCRIBE_PATH = "subscribe";

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
    }

    @Override
    public void run(ServerConfiguration config, Environment env) throws Exception {
        final WriterConfiguration writerConfig = config.getWriterConfiguration();

        final JdbiFactory jdbiFactory = new JdbiFactory();
        final Jdbi jdbi = jdbiFactory.build(env, config.getDatabase(), writerConfig.getTableName());

        final Snowflake idGenerator = new Snowflake(config.getHostId());

        final QDbWriter qDbWriter = new QDbWriter(idGenerator, jdbi, writerConfig);

        final QueueWriter queueWriter = new QueueWriter(writerConfig, qDbWriter);
        env.lifecycle().manage(queueWriter);

        //Thread pool to handle consumer groups
        final ScheduledExecutorService dispatchPool = env.lifecycle()
                .scheduledExecutorService("mesg-dispatcher-%s")
                .threads(config.getMessageDispatcherThreadPoolSize())
                .shutdownTime(config.getShutdownGracePeriod())
                .build();

        final long pollInterval = config.getNewMessagePollInterval().toMilliseconds();

        final ServletContextHandler servletCtxHandler = env.getApplicationContext();
        final WebSocketUpgradeFilter webSocketUpgradeFilter = WebSocketUpgradeFilter.configure(servletCtxHandler);
        final NativeWebSocketConfiguration nativeWebSocketConfiguration = webSocketUpgradeFilter.getConfiguration();

        final WebSocketConfiguration wsConfig = config.getWebSocketConfiguration();
        wsConfig.configureWebSocketPolicy(nativeWebSocketConfiguration.getPolicy());
        nativeWebSocketConfiguration.addMapping(String.format("/%s/*", PUBLISH_PATH),
                new ProducerWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), queueWriter));

        final Cache<Long, String> messageCache = buildMessagesCache(config.getCacheConfiguration());

        // Set up consumer groups WebSocket handlers
        final Map<String, ConsumerGroupConfiguration> cgConfigs = config.getConsumerGroups();
        if (cgConfigs != null) {
            for (Map.Entry<String, ConsumerGroupConfiguration> cg : cgConfigs.entrySet()) {
                final String cgName = cg.getKey();
                final ConsumerGroupConfiguration cgConfig = cg.getValue();
                final QDbReader qDbReader = new QDbReader(qDbWriter, jdbi, writerConfig, cgName, cgConfig, messageCache);
                final ConsumerGroup consumerGroup = new ConsumerGroup(qDbReader);
                nativeWebSocketConfiguration.addMapping(String.format("/%s/%s", DEQUEUE_PATH, cgName),
                        new ConsumerGroupWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), consumerGroup));
                env.lifecycle().manage(consumerGroup);
                dispatchPool.scheduleAtFixedRate(consumerGroup, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
            }
        }

        // Set up topics WebSocket handlers
        final Map<String, TopicConfiguration> topicConfigs = config.getTopics();
        if (topicConfigs != null) {
            for (Map.Entry<String, TopicConfiguration> tpc : topicConfigs.entrySet()) {
                final String topicName = tpc.getKey();
                final TopicConfiguration tpcConfig = tpc.getValue();
                final QDbReader qDbReader = new QDbReader(qDbWriter, jdbi, writerConfig, topicName, tpcConfig, messageCache);
                final Topic topic = new Topic(tpcConfig, qDbReader);
                nativeWebSocketConfiguration.addMapping(String.format("/%s/%s", SUBSCRIBE_PATH, topicName),
                        new TopicSubscriptionWebSocketCreator(wsConfig.getOrigin(), config.getMaxConnections(), topic));
                env.lifecycle().manage(topic);
                dispatchPool.scheduleAtFixedRate(topic, pollInterval, pollInterval, TimeUnit.MILLISECONDS);
            }
        }

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
