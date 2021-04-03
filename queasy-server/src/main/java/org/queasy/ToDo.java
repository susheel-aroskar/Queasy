package org.queasy;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public class ToDo {

    //-  Timeout with #GET
    //-  Integration test(s)
    //-  QueueTrimmerTask using lifecycle scheduler builder
    //-  Health check

    //-  Replication - add partition as non unique key column?
    //      - On detecting dead master insert it's rows in elected replicator's DB with replicators snowflakeIds
    //      - Add column original_id to queue table and populate it with original message ids of dead master
    //      - change message format to <mesg_id>:<original_mesg_id | "">\n<message body>
    //      - return messages from failed over, dead master starting from last persisted ckpt of the dead master
    //      - exchanged heart-beats and checkpoint regularly between connected masters

    //-  Metrics using DW

    //-  Subscriber topic

    //-  #NACK from consumer to push message back on the queue?

}
