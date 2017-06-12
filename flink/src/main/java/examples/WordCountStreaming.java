package examples;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordCountStreaming {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 1111);

        DataStream<Tuple2<String, Integer>> counts =
            // split up the lines in pairs (2-tuples) containing: (word,1)
            text.flatMap(new LineSplitter())
                // group by first item of the tuple
                .keyBy(0)
                // aggregate results in 5-second time windows
                .timeWindow(Time.seconds(5))
                // sum by the second item of the tuple
                .sum(1);

        // emit result
        counts.print();

        // execute program
        env.execute("WordCount Example");
    }
}