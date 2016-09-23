package cn.iot.log;

import java.io.Serializable;

public class FullLogDTO implements Serializable {
	private static final long serialVersionUID = 1L;
	private String time;
	private String day;
	private String threadid;
	private String level;
	private String message;
	private String source;
	private String fullclass;

	public String getFullclass() {
		return fullclass;
	}

	public void setFullclass(String fullclass) {
		this.fullclass = fullclass;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getThreadid() {
		return threadid;
	}

	public void setThreadid(String threadid) {
		this.threadid = threadid;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	@Override
	public String toString() {
		return String.format("time:%s,day:%s,threadid:%s,level:%s,fullclass:%s,message:%s,source:%s", time, day,
				threadid, level, fullclass, message, source);
	}
}
