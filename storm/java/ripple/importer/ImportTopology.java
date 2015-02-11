package ripple.importer;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;

public class ImportTopology {
  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("ledgerStream", new LedgerStreamSpout());
    
    builder.setBolt("transactions", new TransactionBolt(), 6)
      .shuffleGrouping("ledgerStream");
    
    builder.setBolt("exchanges", new ExchangesBolt(), 6)
      .fieldsGrouping("transactions", "exchangeAggregation", new Fields("pair"));
    
    Config conf = new Config();
    //conf.setDebug(true);

    
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      conf.setMessageTimeoutSecs(60);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    
    } else {
      
      //conf.setMaxTaskParallelism(6);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("ledger-import", conf, builder.createTopology());

      //Thread.sleep(10000);
      //cluster.shutdown();
    }
  }
}