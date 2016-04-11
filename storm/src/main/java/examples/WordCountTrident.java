package examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.*;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

import java.io.IOException;

/**
 * Created by davido on 4/10/16.
 */
public class WordCountTrident {


    public static void main(String[] args) throws IOException, InterruptedException {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        LocalDRPC drpc = new LocalDRPC();

        TridentState state = topology
                .newStream("spout", spout)
                .parallelismHint(2)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(2);

//        state.newValuesStream()
//                .each(new Fields("word", "count"), new Debug("++++ count"));

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        Config conf = new Config();
        //conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, topology.build());

        Thread.sleep(5000);

        System.out.println("Count of the words [the, you, to, and] : " +
                drpc.execute("words", "the you to and"));

        System.in.read();
        cluster.shutdown();
    }
}
