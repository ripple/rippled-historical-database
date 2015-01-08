package importer;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class TransactionBolt extends ShellBolt implements IRichBolt {
	
  public TransactionBolt() {
    super("node", "importer/transactionBolt.js");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("close_time"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}