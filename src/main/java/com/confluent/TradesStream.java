package com.confluent;

import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class TradesStream {
    public static final String TRADES_TOPIC = "dedup-output";
    public static final String TRADES_REL_TOPIC = "test-rahul-with-dups";
    public static final String STORE_NAME = "test";
    public static final String OUTPUT_TOPIC = "output";
    final StreamsBuilder builder = new StreamsBuilder();
    ObjectMapper mapper = new ObjectMapper();

    public Topology getTopology(Properties settings){
         // Default serde for keys of data records (here: built-in serde for String type)
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Serde<JsonNode> jsonNodeSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // Default serde for values of data records (here: built-in serde for Long type)
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        StreamsConfig config = new StreamsConfig(settings);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> tradesRelStream = builder.stream(TRADES_REL_TOPIC, Consumed.with(Serdes.String(), jsonNodeSerde));

        tradesRelStream.flatMap((key, value)->{
                    List<KeyValue<String, JsonNode>> keyValueList = new ArrayList<KeyValue<String, JsonNode>>();
                    ObjectNode expectedUnderlyingTrades = (ObjectNode)value.get("expectedUnderlyingTrades");
                    Iterator<String> tradeIds = expectedUnderlyingTrades.fieldNames();
                    while(tradeIds.hasNext()){
                        String tradeId = tradeIds.next();
                        keyValueList.add(new KeyValue<String, JsonNode>(tradeId,value));
                    }
                    return keyValueList;
                })
                .groupByKey()
                .reduce((oldValue, newValue)->{
                    ObjectNode objectNode = mapper.createObjectNode();
                    try {
                        objectNode.put(oldValue.get("tradeRelID").asText(), oldValue);
                    }catch (NullPointerException ne){

                    }
                     objectNode.put(newValue.get("tradeRelID").asText(), newValue);

                    return objectNode;
                },Materialized.with(Serdes.String(), jsonNodeSerde))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonNodeSerde));


        Topology topology = builder.build();
        return topology;
    }

}
