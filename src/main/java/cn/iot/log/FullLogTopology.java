package cn.iot.log;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 * 
 */
public class FullLogTopology {
	private static final Logger logger = LoggerFactory.getLogger(FullLogTopology.class);

	public static void main(String[] args) throws Exception {
		KafkaSpout kafkaSpout = createKafkaSpout();
		EsIndexBolt esIndexBolt = createESBolt();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 4);
		builder.setBolt("fulllog", new FullLogBolt(), 10).shuffleGrouping("spout");
//		builder.setBolt("printlog", new PrintBolt(), 4).shuffleGrouping("fulllog");
		builder.setBolt("esbolt", esIndexBolt, 4).shuffleGrouping("fulllog");

		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-log", conf, builder.createTopology());
		// Thread.sleep(6000000);
		// cluster.shutdown();
	}

	private static KafkaSpout createKafkaSpout() {
		ZkHosts hosts = new ZkHosts("192.168.156.60:2181", "/kafka/brokers");
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "apiserver_log_test", "/storm-log", "1365055");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return new KafkaSpout(spoutConfig);
	}

	private static EsIndexBolt createESBolt() {
		Map<String, String> additionalParameters = new HashMap<>();
		additionalParameters.put("client.transport.sniff", "true");
		additionalParameters.put("node.name", "storm-es-client");
		additionalParameters.put("transport.netty.connect_timeout", "10000ms");
		EsConfig esConfig = new EsConfig("LOG-CLUSTER", new String[] { "192.168.156.60:9300" }, additionalParameters);
		EsTupleMapper tupleMapper = new FullLogESTupleMapper();
		EsIndexBolt indexBolt = new EsIndexBolt(esConfig, tupleMapper);
		return indexBolt;
	}
}
