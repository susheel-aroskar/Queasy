package org.queasy.db;

import com.google.common.collect.ImmutableList;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.jdbi.v3.core.Jdbi;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.queasy.ServerApplication;
import org.queasy.ServerConfiguration;

import java.io.File;

/**
 * @author saroskar
 * Created on: 2021-03-23
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class QueueSchemaTest {

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            ServerApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml")
    );

    public static final String EXPECTED_INSERT_SQL = "INSERT INTO qeasy_q " +
            "(id, type, mesg_text, mesg_bin, event, name, category, priority, status, v1, v2) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static final String EXPECTED_SELECT_SQL_1 = "SELECT * FROM qeasy_q WHERE id > ? AND id <= ? LIMIT ?";

    public static final String EXPECTED_SELECT_SQL_2 = "SELECT * FROM qeasy_q WHERE id > ? AND id <= ? " +
            "AND event = 'payment' and priority='10' LIMIT ?";

    @BeforeAll
    public static void setup() throws Exception {
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        final String queueSchemaPath = ResourceHelpers.resourceFilePath("test_queue_schema.yml");
        app.getApplication().run("db", "migrate", "--migrations", queueSchemaPath, configPath);
    }

    @Test
    public void test() {
        final ServerConfiguration config = app.getConfiguration();
        final JdbiFactory jdbiFactory = new JdbiFactory();
        final Jdbi jdbi = jdbiFactory.build(app.getEnvironment(), config.getDatabase(), config.getQueue().getName());
        final QueueSchema qs = new QueueSchema(jdbi, app.getConfiguration().getQueue());
        assertEquals(EXPECTED_INSERT_SQL, qs.getInsertStmt());
        assertEquals(EXPECTED_SELECT_SQL_1, qs.builSelectQuery(""));
        assertEquals(EXPECTED_SELECT_SQL_1, qs.builSelectQuery(null));
        assertEquals(EXPECTED_SELECT_SQL_2, qs.builSelectQuery("event = 'payment' and priority='10'"));
        assertEquals(ImmutableList.of("event","name","category","priority","status","v1","v2"), qs.getUserDefinedColumns());
    }

    @AfterAll
    public static void destroy() {
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();
    }
}
