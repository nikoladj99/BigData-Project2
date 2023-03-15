package proj;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;
import java.util.Properties;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction.Context;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import java.util.PriorityQueue;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

import java.util.*;

public class App {

    public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputTopic = "topic7";
        String server = "kafka:9092";

        DataStream<CheckIn> dataStream = StreamConsumer(inputTopic, server, environment);

// Minimalna i maksimalna vrednost za atrbiut 'time_spent' na odredjenoj lokaciji
        SingleOutputStreamOperator groupedStream = dataStream
        .keyBy(CheckIn::getLocation_id)
        .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .aggregate(new MinMaxAggregateFunction());
        
		groupedStream.print();

        CassandraSink.addSink(groupedStream)
				.setHost("cassandra")
                .setQuery("INSERT INTO flink.min_max(location_id, max, min) values (?, ?, ?);")
				.build();

// Srednja vrednost za atrbiut 'time_spent' na odredjenoj lokaciji
        SingleOutputStreamOperator meanStream = dataStream
        .keyBy(CheckIn::getLocation_id)
        .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .aggregate(new MeanAggregate());
        
		meanStream.print();

        CassandraSink.addSink(meanStream)
				.setHost("cassandra")
                .setQuery("INSERT INTO flink.mean(location_id, mean) values (?, ?);")
				.build();

// Broj pojavljivanja korisnika na odredjenoj lokaciji

        DataStream<Tuple2<String, Integer>> location_user_counts = dataStream
        .map(new MapFunction<CheckIn, String>() {
            @Override
            public String map(CheckIn value) throws Exception {
                return value.getLocation_id() + "-" + value.getUser();
            }
        })
        .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        })
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
        //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .sum(1);

        location_user_counts.print();

        CassandraSink.addSink(location_user_counts)
				.setHost("cassandra")
                .setQuery("INSERT INTO flink.location_user_counts(user_location, counts) values (?, ?);")
				.build();

// Top N lokacija u odnosu na broj poseta

        int N = 5;

        DataStream<Tuple2<String, Integer>> topN = dataStream
        .map(new MapFunction<CheckIn, String>() {
            @Override
            public String map(CheckIn value) throws Exception {
                return value.getLocation_id();
            }
        })
        .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        })
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
        //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .sum(1)
        //.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                PriorityQueue<Tuple2<String, Integer>> queue = new PriorityQueue<>(Comparator.comparingInt(o -> o.f1));
                for (Tuple2<String, Integer> t : iterable) {
                    queue.offer(t);
                    if (queue.size() > N) {
                        queue.poll();
                    }
                }
                List<Tuple2<String, Integer>> topN = new ArrayList<>(queue);
                topN.sort(Comparator.comparingInt(o -> -o.f1));
                for (Tuple2<String, Integer> t : topN) {
                    collector.collect(t);
                }
            }
        });

        topN.print();

        CassandraSink.addSink(topN)
				.setHost("cassandra")
                .setQuery("INSERT INTO flink.top_n_locations(location_id, counts) values (?, ?);")
				.build();

		environment.execute();
    }

	public static DataStream<CheckIn> StreamConsumer(String inputTopic, String server, StreamExecutionEnvironment environment) throws Exception {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);

        return stringInputStream.map(new MapFunction<String, CheckIn>() {
            private static final long serialVersionUID = -999736771747691234L;

			@Override
            public CheckIn map(String value) throws Exception {
                String[] split = value.split(",");
                return new CheckIn(
                        Integer.parseInt(split[0]),
                        split[1],
                        Double.parseDouble(split[2]),
                        Double.parseDouble(split[3]),
                        split[4],
                        Integer.parseInt(split[5])
                );
            }
        });
    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        return consumer;
    }
}
