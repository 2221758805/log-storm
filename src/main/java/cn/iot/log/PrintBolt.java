package cn.iot.log;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(PrintBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Object obj = input.getValue(0);
		// if (obj instanceof Map<?, ?>) {
		// Map<String, Object> fullLog = (Map<String, Object>) obj;
		// logger.info("time:{},day:{},level:{},threadid:{},message:{}",
		// fullLog.get("time"), fullLog.get("day"),
		// fullLog.get("level"), fullLog.get("threadid"),
		// fullLog.get("message"));
		// }
		if (obj instanceof FullLogDTO) {
			FullLogDTO fullLog = (FullLogDTO) obj;
			logger.info("time:{},day:{},level:{},threadid:{},message:{}", fullLog.getTime(), fullLog.getDay(),
					fullLog.getLevel(), fullLog.getThreadid(), fullLog.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
