package org.queasy;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public class ToDo {

    //-  TopicSubscriber topic (Tests)
    //-  QueueTrimmerTask using lifecycle scheduler builder
    //-  Integration test(s)
    //-  Health check

    //-  Replication:
    //      - On detecting dead master insert it's rows in elected replicator's DB with replicators snowflakeIds
    //      - Add column original_id to queue table and populate it with original message ids of dead master
    //      - change message format to <mesg_id>:<original_mesg_id | "">\n<message body>
    //      - return messages from failed over, dead master starting from last persisted ckpt of the dead master
    //      - exchanged heart-beats and checkpoint regularly between connected masters
    //      - insert checkpoints in normal message streams with qname replaced with "_ckpt"?

    //-  Metrics using DW
    //-  Make sure SQLite is in WAL mode (PRAGMA synchronous=OFF ?)

}
