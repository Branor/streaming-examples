package examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LineTokenizer implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) {
        // normalize and split the line into words
        String[] tokens = value.toLowerCase().split("\\W+");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(token);
            }
        }
    }
}