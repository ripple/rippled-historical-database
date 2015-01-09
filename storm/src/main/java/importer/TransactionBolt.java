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
    declarer.declareStream("payments", new Fields("payment"));
    declarer.declareStream("exchanges", new Fields("exchange"));
    declarer.declareStream("balance_changes", new Fields("change"));
    declarer.declareStream("accounts_created", new Fields("account"));
    declarer.declareStream("affected_accounts", new Fields("account"));
    declarer.declareStream("memos", new Fields("memo"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}