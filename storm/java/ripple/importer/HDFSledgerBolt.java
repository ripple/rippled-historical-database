package ripple.importer;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class HDFSledgerBolt extends ShellBolt implements IRichBolt {

  public HDFSledgerBolt() {
    super("node", "HDFSledgerBolt.js");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
