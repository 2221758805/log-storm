package cn.iot.log;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullLogBolt extends BaseBasicBolt {
	private static final Logger logger = LoggerFactory.getLogger(FullLogBolt.class);
	private static final long serialVersionUID = 1L;

	private static final String REGEX = "(?<time>^(?<day>\\d{4}-\\d{2}-\\d{2})\\s\\d{2}:\\d{2}:\\d{2},\\d{3})\\s\\d+\\s\\[(?<threadid>[\\w\\-]+)\\]\\s(?<level>\\w+)\\s+(?<fullclass>[\\. \\w]+)\\s-\\s(?<message>.+)";
	private String KEYS[] = { "time", "day", "threadid", "level", "fullclass", "message" };

	private ThreadLocal<Matcher> localMatcher;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		localMatcher = new ThreadLocal<>();
		super.prepare(stormConf, context);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String content = input.getString(0);
		if (content == null || content.length() < 1) {
			return;
		}
		// Map<String, Object> dataMap = parseContent(content);
		// if (dataMap == null) {
		// return;
		// }
		// collector.emit(new Values(dataMap));
		FullLogDTO dto = parserLog(content);
		if (dto == null) {
			return;
		}
		logger.debug("parsed fulllog:{}", dto.toString());
		collector.emit(new Values(dto));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("fulllog"));
	}

	private Map<String, Object> parseContent(String content) {
		Matcher matcher = getMatcher();
		boolean isMatch = matcher.reset(content).matches();
		if (isMatch) {
			Map<String, Object> dataMap = new HashMap<String, Object>();
			for (String key : KEYS) {
				String value = matcher.group(key);
				dataMap.put(key, value);
			}
			dataMap.put("source", content);
			return dataMap;
		}
		return null;
	}

	private FullLogDTO parserLog(String content) {
		Matcher matcher = getMatcher();
		boolean isMatch = matcher.reset(content).matches();
		if (isMatch) {
			FullLogDTO dto = new FullLogDTO();
			for (String key : KEYS) {
				String value = matcher.group(key);
				if (key.equals("time")) {
					dto.setTime(value);
				}
				if (key.equals("day")) {
					dto.setDay(value);
				}
				if (key.equals("threadid")) {
					dto.setThreadid(value);
				}
				if (key.equals("level")) {
					dto.setLevel(value);
				}
				if (key.equals("fullclass")) {
					dto.setFullclass(value);
				}
				if (key.equals("message")) {
					dto.setMessage(value);
				}
			}
			dto.setSource(content);
			return dto;
		} else {
			return null;
		}
	}

	private Matcher getMatcher() {
		/*
		 * if (localMatcher == null) { localMatcher = new ThreadLocal<>(); }
		 */
		Matcher matcher = localMatcher.get();
		if (matcher == null) {
			Pattern pattern = Pattern.compile(REGEX);
			matcher = pattern.matcher("");
			localMatcher.set(matcher);
		}
		return matcher;
	}
}
