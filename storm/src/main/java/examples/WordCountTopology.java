package examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.io.IOException;

/**
 * Created by davido on 4/10/16.
 */
public class WordCountTopology {


    public static void main(String[] args) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 5);

        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("count");

        Config conf = new Config();
        //conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());

        System.in.read();
        cluster.shutdown();
    }
}
