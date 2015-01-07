package importer;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

public class ImportTopology {
  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("ledgerStream", new LedgerStreamSpout());
    builder.setBolt("saveLedger", new SaveLedgerBolt(), 2).shuffleGrouping("ledgerStream");

    Config conf = new Config();
    //conf.setDebug(true);

    
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    
    } else {
      
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("ledger-import", conf, builder.createTopology());

      //Thread.sleep(10000);
      //cluster.shutdown();
    }
  }
}