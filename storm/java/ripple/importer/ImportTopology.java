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

    builder.setBolt("transactions", new TransactionBolt(), 20)
      .shuffleGrouping("ledgerStream", "txStream");

    builder.setBolt("exchanges", new ExchangesBolt(), 10)
      .fieldsGrouping("transactions", "exchangeAggregation", new Fields("pair"));

    builder.setBolt("stats", new StatsBolt(), 2)
      .fieldsGrouping("transactions", "statsAggregation", new Fields("label"))
      .fieldsGrouping("ledgerStream", "statsAggregation", new Fields("label"));

    builder.setBolt("accountPayments", new AccountPaymentsBolt(), 2)
      .fieldsGrouping("transactions", "accountPaymentsAggregation", new Fields("account"));

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
