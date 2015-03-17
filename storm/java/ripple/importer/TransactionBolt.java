package ripple.importer;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class TransactionBolt extends ShellBolt implements IRichBolt {

  public TransactionBolt() {
    super("node", "src/transactionBolt.js");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("paymentAggregation",  new Fields("payment"));
    declarer.declareStream("exchangeAggregation", new Fields("exchange", "pair"));
    declarer.declareStream("statsAggregation",    new Fields("stat", "label"));
    declarer.declareStream("accountPaymentsAggregation", new Fields("payment", "account"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
