package com.confluent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.TopLevelAttribute;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.Test;
import org.junit.Assert.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Properties;

public class TradeStreamTest {
    TradesStream tradesStream = new TradesStream();
    @Test
    public void testTrades() throws JsonProcessingException {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        Topology topology = tradesStream.getTopology(props);
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        TestInputTopic<String, JsonNode> tradeRelTopic = testDriver.createInputTopic(TradesStream.TRADES_REL_TOPIC, Serdes.String().serializer(), new JsonSerializer());
        TestInputTopic<String, JsonNode> tradeTopic = testDriver.createInputTopic(TradesStream.TRADES_TOPIC, Serdes.String().serializer(), new JsonSerializer());
        String tradeString11321 = "{\n" +
                "\n" +
                "  \"tradeEventId\" : \"681947\",\n" +
                "\n" +
                "  \"tradeId\" : \"605:-11321:1:BRS\",\n" +
                "\n" +
                "  \"Status\" : \"PROCESSED\",\n" +
                "\n" +
                "  \"sourceDataRecordId\" : 0,\n" +
                "\n" +
                "  \"sourceDataSetId\" : 0,\n" +
                "\n" +
                "  \"status\" : \"PROCESSED\"\n" +
                "\n" +
                "}";

        String tradeString10903 = "{\n" +
                "\n" +
                "  \"tradeEventId\" : \"681948\",\n" +
                "\n" +
                "  \"tradeId\" : \"605:-10903:2:BRS\",\n" +
                "\n" +
                "  \"Status\" : \"PROCESSED\",\n" +
                "\n" +
                "  \"sourceDataRecordId\" : 0,\n" +
                "\n" +
                "  \"sourceDataSetId\" : 0,\n" +
                "\n" +
                "  \"status\" : \"PROCESSED\"\n" +
                "\n" +
                "}";


        String tradeRelString = "{\n" +
                "\n" +
                "  \"tradeRelID\" : \"682069\",\n" +
                "\n" +
                "  \"tradeRelType\" : \"PO\",\n" +
                "\n" +
                "  \"expectedCountUnderlyingTrades\" : 2,\n" +
                "\n" +
                "  \"Status\" : \"PROCESSED\",\n" +
                "\n" +
                "  \"expectedUnderlyingTrades\" : {\n" +
                "\n" +
                "    \"605:-10903:2:BRS\" : {\n" +
                "\n" +
                "      \"tradeEventId\" : \"682070\",\n" +
                "\n" +
                "      \"tradeId\" : \"605:-10903:2:BRS\",\n" +
                "\n" +
                "      \"Status\" : \"PROCESSED\",\n" +
                "\n" +
                "      \"sourceDataRecordId\" : 848984,\n" +
                "\n" +
                "      \"sourceDataSetId\" : 848984,\n" +
                "\n" +
                "      \"status\" : \"PROCESSED\"\n" +
                "\n" +
                "    },\n" +
                "\n" +
                "    \"605:-11321:1:BRS\" : {\n" +
                "\n" +
                "      \"tradeEventId\" : \"682071\",\n" +
                "\n" +
                "      \"tradeId\" : \"605:-11321:1:BRS\",\n" +
                "\n" +
                "      \"Status\" : \"PROCESSED\",\n" +
                "\n" +
                "      \"sourceDataRecordId\" : 848984,\n" +
                "\n" +
                "      \"sourceDataSetId\" : 848984,\n" +
                "\n" +
                "      \"status\" : \"PROCESSED\"\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "  },\n" +
                "\n" +
                "  \"reportedProcessedTrades\" : null,\n" +
                "\n" +
                "  \"sourceDataRecordId\" : 0,\n" +
                "\n" +
                " \"sourceDataSetId\" : 0,\n" +
                "\n" +
                "  \"status\" : \"PROCESSED\"\n" +
                "\n" +
                "}\n" +
                "\n";
        JsonNode tradeRel = new ObjectMapper().readTree(tradeRelString);
        tradeRelTopic.pipeInput("682069", tradeRel);

        JsonNode trade = new ObjectMapper().readTree(tradeString11321);
        tradeTopic.pipeInput("605:-11321:1:BRS", trade);


        JsonNode trade2 = new ObjectMapper().readTree(tradeString10903);
        tradeTopic.pipeInput("605:-10903:2:BRS", trade);

        TestOutputTopic<String, JsonNode> outputTopic = testDriver.createOutputTopic(TradesStream.OUTPUT_TOPIC, Serdes.String().deserializer(), new JsonDeserializer());
        assertEquals(outputTopic.getQueueSize(), 2);
        List<KeyValue<String, JsonNode>> keyValueList = outputTopic.readKeyValuesToList();
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("605:-10903:2:BRS", tradeRel));

    }


}
