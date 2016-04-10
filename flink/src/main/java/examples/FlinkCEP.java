package examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class FlinkCEP {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9999)
                .flatMap(new LineTokenizer());

        text.print();

        Pattern<String, String> pattern =
                Pattern.<String>begin("start").where(txt -> txt.equals("a"))
                       .next("middle").where(txt -> txt.equals("b"))
                       .followedBy("end").where(txt -> txt.equals("c")).within(Time.seconds(1));

        PatternStream<String> patternStream = CEP.pattern(text, pattern);

        DataStream<String> alerts = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, String> matches) throws Exception {
                return "Found: " +
                        matches.get("start") + "->" +
                        matches.get("middle") + "->" +
                        matches.get("end");
            }
        });

        // emit result
        alerts.print();

        // execute program
        env.execute("WordCount Example");
    }
}