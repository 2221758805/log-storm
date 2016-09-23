package cn.iot.log;

import java.util.UUID;

import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.tuple.ITuple;

import com.alibaba.fastjson.JSON;

public class FullLogESTupleMapper implements EsTupleMapper {
	private static final long serialVersionUID = 1L;

	@Override
	public String getSource(ITuple tuple) {
		Object obj = tuple.getValue(0);
		if (obj instanceof FullLogDTO) {
			FullLogDTO fullLog = (FullLogDTO) obj;
			return JSON.toJSONString(fullLog);
		}
		return null;
	}

	@Override
	public String getIndex(ITuple tuple) {
		Object obj = tuple.getValue(0);
		if (obj instanceof FullLogDTO) {
			FullLogDTO fullLog = (FullLogDTO) obj;
			String index = String.format("apiserver_full_log%s", fullLog.getDay());
			return index;
		}
		return null;
	}

	@Override
	public String getType(ITuple tuple) {
		return "log";
	}

	@Override
	public String getId(ITuple tuple) {
		return UUID.randomUUID().toString();
	}

}
