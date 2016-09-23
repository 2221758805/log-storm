package cn.iot.log;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 * 
 */
public class FullLogTopology {
	private static final Logger logger = LoggerFactory.getLogger(FullLogTopology.class);

	public static void main(String[] args) throws Exception {
		ZkHosts hosts = new ZkHosts("192.168.156.60:2181", "/kafka/brokers");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "apiserver_log_test", "/storm-log", "1365055");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 4);
//		builder.setBolt("echo", new EchoLogBolt(), 10).shuffleGrouping("spout");
		builder.setBolt("fulllog", new FullLogBolt(), 10).shuffleGrouping("spout");
		builder.setBolt("printlog", new PrintBolt(), 4).shuffleGrouping("fulllog");
		
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-log", conf, builder.createTopology());
//		Thread.sleep(6000000);
//		cluster.shutdown();
	}

	public static class EchoLogBolt extends BaseBasicBolt {
		private static final long serialVersionUID = 1L;

		public void execute(Tuple input, BasicOutputCollector collector) {
			String log = input.getString(0);
			logger.info("mylog:{}", log);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
	}
}
