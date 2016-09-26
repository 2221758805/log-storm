package cn.iot.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.StormTopology;
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
		builder.setBolt("printlog", new PrintBolt(), 4).shuffleGrouping("fulllog");
		builder.setBolt("esbolt", esIndexBolt, 4).shuffleGrouping("fulllog");
		StormTopology topology = builder.createTopology();
		startClusterTopology(topology);
//		startLocalTopology(topology);
	}

	private static void startLocalTopology(StormTopology topology) throws Exception {
		logger.info("Begin to start local topology.......");
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-log", config, topology);
		Thread.sleep(60000);
		cluster.shutdown();
		logger.info("Finish start local topology");
	}

	private static void startClusterTopology(StormTopology topology) throws Exception {
		logger.info("Begin to start cluster topology.......");
		Config config = new Config();
		config.put(Config.NIMBUS_SEEDS, parseNimbus("192.168.156.60"));
		config.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt("6627"));
		config.put(Config.STORM_ZOOKEEPER_PORT, parseZkPort("192.168.156.58:2181,192.168.156.60:2181"));
		config.put(Config.STORM_ZOOKEEPER_SERVERS, parseZkHosts("192.168.156.58:2181,192.168.156.60:2181"));
		config.setNumWorkers(10);
		config.setMaxSpoutPending(5000);
		StormSubmitter.submitTopology("storm-log", config, topology);
		logger.info("Finish start cluster topology");
	}

	private static List<String> parseNimbus(String nimbus){
		String[] hostsAndPorts = nimbus.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);
		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}
	
	private static List<String> parseZkHosts(String zkNodes) {
		String[] hostsAndPorts = zkNodes.split(",");
		List<String> hosts = new ArrayList<String>(hostsAndPorts.length);
		for (int i = 0; i < hostsAndPorts.length; i++) {
			hosts.add(i, hostsAndPorts[i].split(":")[0]);
		}
		return hosts;
	}

	private static int parseZkPort(String zkNodes) {
		String[] hostsAndPorts = zkNodes.split(",");
		int port = Integer.parseInt(hostsAndPorts[0].split(":")[1]);
		return port;
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
