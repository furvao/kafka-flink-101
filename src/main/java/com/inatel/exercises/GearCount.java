package com.inatel.exercises;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class GearCount {

  public static void main(String[] args) throws Exception {
	  
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new CountGearChange())
            .flatMap(new CountMapper())
            .map(new CountPrinter())
            .print();

    env.execute();
    }

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Integer,Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Integer,Integer>> out) throws Exception {
          String carNumber = "car" + jsonTelemetry.get("Car").asText();
          Integer gear =  jsonTelemetry.get("telemetry").get("Gear").intValue();;
          out.collect(new Tuple3<>(carNumber,  gear,  0));
      }
    }

    // Reduce Function - Sum samples and count
    // This funciton return, for each car, the gear change count.
    // The counter is used for the count gear change calculation.
    static class CountGearChange implements ReduceFunction<Tuple3<String, Integer,Integer>> {

       @Override
      public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer,Integer> value1, Tuple3<String, Integer, Integer> value2) {
        int countGearChange = value1.f2;
        if(value1.f1 != value2.f1){
            countGearChange = countGearChange +1;
        }
        
        return new Tuple3<>(value1.f0, value2.f1, countGearChange);
      }
    }

    // FlatMap Function - Count
    // return  gear count converted
    static class CountMapper implements FlatMapFunction<Tuple3<String, Integer,Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Integer, Integer> carInfo, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(  new Tuple2<>( carInfo.f0 , carInfo.f2 )  );
      }
    }

    // Map Function - Print gear count    
    static class CountPrinter implements MapFunction<Tuple2<String, Integer>, String> {
     @Override
      public String map(Tuple2<String, Integer> countEntry) throws Exception {
         if(countEntry.f0.intern()=="car5")
        {
          return  String.format(" %s : %d ", countEntry.f0 , countEntry.f1 ) ;
        }
        return "...";
      }
    }

  }