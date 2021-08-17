package com.confluent;

import com.confluent.avro.Trade;
import com.confluent.avro.TradeRel;
import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

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
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        settings.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        settings.put("schema.registry.url", "http://my-schema-registry:8081");
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://my-schema-registry:8081");
        Serde<Trade> avroTradeSerde = new SpecificAvroSerde<>();
        Serde<TradeRel> avroTradeRelSerde = new SpecificAvroSerde<>();
        // Configure Serdes to use the same mock schema registry URL

        avroTradeSerde.configure(serdeConfig, false);
        avroTradeRelSerde.configure(serdeConfig, false);


        StreamsConfig config = new StreamsConfig(settings);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TradeRel> tradesRelStream = builder.stream(TRADES_REL_TOPIC, Consumed.with(Serdes.String(), avroTradeRelSerde));
        KTable<String, Trade> tradesTable = builder.table(TRADES_TOPIC,Consumed.with(Serdes.String(), avroTradeSerde), Materialized.as("trades-table"));

        KTable<String, Map<String,TradeRel>> tradesRelTable = tradesRelStream.flatMap((key, value)->{
                    List<KeyValue<String, Map<String, TradeRel>>> keyValueList = new ArrayList<>();
                    Map<String, Trade> expectedUnderlyingTrades = value.getExpectedUnderlyingTrades();
                    Iterator<String> tradeIds = expectedUnderlyingTrades.keySet().iterator();
                    while(tradeIds.hasNext()){
                        String tradeId = tradeIds.next();

                        Map<String, TradeRel> underlyingtrades = new HashMap<>();
                        underlyingtrades.put(tradeId, value);
                        //tempTradel.setExpectedUnderlyingTrades(underlyingtrades);
                        keyValueList.add(new KeyValue<String,Map<String,TradeRel>>(tradeId,underlyingtrades));
                    }
                    return keyValueList;
                })
                .groupByKey()
                .reduce((oldValue, newValue)->{
                    //During this Aggregate/Combine diff events of the same key (same Fnd+Inv+Ver+Src, combine their respective Underlying Trades Maps

                    oldValue.putAll(newValue);


                    return oldValue;

                }, Materialized.as("trades-rel-table"));


        tradesTable.join(tradesRelTable,
                (tradeData, tradeRelData) -> {
                  Iterator<String> tradeIds = tradeRelData.keySet().iterator();

                    while(tradeIds.hasNext()){
                       String tradeId = tradeIds.next();

                        TradeRel tradeDataWithoutReported = tradeRelData.get(tradeId);
                        Map<String, Trade> tradeReportedData = new HashMap<>();
                        tradeReportedData.put(tradeData.getTradeId(), tradeData);


                        tradeDataWithoutReported.setReportedProcessedTrades(tradeReportedData);


                   }
                   return tradeRelData;
                }, Materialized.as("trades-rel-trade-joined-table"))
                .toStream()
                .flatMap((key, value) -> {
                    List<KeyValue<String, TradeRel>> keyValueList = new ArrayList<>();
                    Iterator<String> tradeIds = value.keySet().iterator();
                    while(tradeIds.hasNext()) {
                        String tradeId = tradeIds.next();
                        keyValueList.add(new KeyValue<>(tradeId, value.get(tradeId)));

                    }
                    return  keyValueList;
                })
                .groupByKey()
                .reduce((oldValue, newValue)->{
                    Map<String, Trade> reportedTrades = oldValue.getReportedProcessedTrades();
                    reportedTrades.putAll(newValue.getReportedProcessedTrades());
                    return oldValue;
                }, Materialized.as("processed-trade-table"))
                .toStream()
                .filter((key,value)->{
                    int expected = value.getExpectedUnderlyingTrades().size();
                    Iterator<String> reportedTrades = value.getReportedProcessedTrades().keySet().iterator();
                    int count=0;
                    while(reportedTrades.hasNext())
                    {
                        count++;
                        reportedTrades.next();
                    }

                    return count==expected;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), avroTradeRelSerde));


        Topology topology = builder.build();
        System.out.println(topology.describe());
        return topology;
    }

    public static void main(String[] args) {

    }

}
