package com.ywwl.kafka;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSONObject;

import ch.qos.logback.classic.Level;

public class KafkaLogger {
	private Producer<String, String> producer;
	private String appName;
	private Level effectiveLevel = Level.ALL;
	private boolean isSystemOut = false;

	public KafkaLogger() throws Exception {
		Properties props = new Properties();
		String fileName = "kafkaProducer.properties";
		try {
			InputStream resourceAsStream = this.getClass().getResourceAsStream("/" + fileName);
			props.load(resourceAsStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			FileInputStream fileInputStream = new FileInputStream(fileName);
			props.load(fileInputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		init(props);
	}

	public KafkaLogger(Properties props) throws Exception {
		init(props);
	}

	private void init(Properties props) throws Exception {
		check(props);
		String name = props.getProperty("appName");
		this.appName = name.toLowerCase();
		props.remove("appName");
		String level = props.getProperty("logLevel");
		props.remove("logLevel");
		setEffectiveLevel(Level.toLevel(level));
		String issystemOut = props.getProperty("issystemOut");
		props.remove("issystemOut");
		setSystemOut(issystemOut);
		this.producer = new KafkaProducer<>(props);
		Runtime.getRuntime().addShutdownHook(new HookThread());
	}

	// bootstrap.servers=56.56.59.26:9092
	// acks=all
	// retries=0
	// batch.size=16384
	// linger.ms=1
	// buffer.memory=33554432
	// max.block.ms=0
	// key.serializer=org.apache.kafka.common.serialization.StringSerializer
	// value.serializer=org.apache.kafka.common.serialization.StringSerializer
	// appName=interfaceForward
	// logLevel=ALL
	private static final String propsKyes[] = { "bootstrap.servers", "acks", "retries", "batch.size", "linger.ms",
			"buffer.memory", "key.serializer", "value.serializer", "appName", "logLevel" };

	private void check(Properties props) throws Exception {
		System.out.println("KafkaLoger 必要配置检查 ： ");
		for (int i = 0; i < propsKyes.length; i++) {
			String key = propsKyes[i];
			String property = props.getProperty(key);
			System.out.println(key + "=" + property);
			if (StringUtils.isEmpty(property)) {
				throw new Exception("KafkaLoger 配置缺少必要字段 : " + key);
			}
		}
	}

	/**
	 * 用于在程序退出时释放资源
	 * 
	 * @author djl20
	 *
	 */
	class HookThread extends Thread {
		@Override
		public void run() {
			if (producer != null) {
				producer.close();
			}
		}
	}

	public void info(Object message, Map<String, Object> otherField, String appNameTail) {
		JSONObject jsonObject = getStackInfos(appNameTail);
		send(message, Level.INFO, jsonObject, otherField);
	}

	// public void debug(Object message) {
	// send(message, Level.DEBUG);
	// }

	public void debug(Object message, Map<String, Object> otherField, String appNameTail) {
		JSONObject jsonObject = getStackInfos(appNameTail);
		send(message, Level.DEBUG, jsonObject, otherField);
	}

	// public void error(Object message) {
	// send(message, Level.ERROR);
	// }

	public void error(Object message, Map<String, Object> otherField, String appNameTail) {
		JSONObject jsonObject = getStackInfos(appNameTail);
		send(message, Level.ERROR, jsonObject, otherField);
	}

	// public void error(Exception e) {
	// String m = getErrorMessage(e.getClass().getName() + e.getMessage(), e);
	// send(m, Level.ERROR);
	// }

	public void error(Exception e, Map<String, Object> otherField, String appNameTail) {
		String m = getErrorMessage(e.getClass().getName() + e.getMessage(), e);
		JSONObject jsonObject = getStackInfos(appNameTail);
		send(m, Level.ERROR, jsonObject, otherField);
	}

	// public void error(Object message, Throwable t) {
	// String m = getErrorMessage(message, t);
	// send(m, Level.ERROR);
	// }

	public void error(Object message, Throwable t, Map<String, Object> otherField, String appNameTail) {
		String m = getErrorMessage(message, t);
		JSONObject jsonObject = getStackInfos(appNameTail);
		send(m, Level.ERROR, jsonObject, otherField);
	}

	private String getErrorMessage(Object message, Throwable t) {
		String m = message != null ? message.toString() : "null";
		if (t != null) {
			StackTraceElement[] stacks = t.getStackTrace();
			for (int i = 0; i < Math.min(stacks.length, 6); i++) {
				m = m + "\n" + stacks[i].toString();
			}
		}
		return m;
	}

	private void send(Object o, Level level, JSONObject jsonObject, Map<String, Object> otherField) {
		if (level.levelInt >= getEffectiveLevel().levelInt) {
			if (otherField != null) {
				jsonObject.putAll(otherField);
			}
			jsonObject.put("message", o.toString());
			jsonObject.put("level", level.levelStr);
			String jsonString = jsonObject.toJSONString();
			producer.send(new ProducerRecord<String, String>("javalog", jsonString));
			if (isSystemOut) {
				System.out.println(jsonString);
			}
		}
	}

	private JSONObject getStackInfos(String appNameTail) {
		StackTraceElement[] stacks = new Throwable().getStackTrace();
		JSONObject jsonObject = new JSONObject();
		if (appNameTail != null) {
			appNameTail = appNameTail.toLowerCase().trim();
		}
		if (!StringUtils.isEmpty(appNameTail)) {
			jsonObject.put("appName", appName + "-" + appNameTail);
		} else {
			jsonObject.put("appName", appName);
		}
		jsonObject.put("javaClass", stacks[2].getClassName());
		jsonObject.put("javaMethod", stacks[2].getMethodName());
		jsonObject.put("codeLine", stacks[2].getLineNumber());
		return jsonObject;
	}

//	public static void main(String[] args) throws Exception {
//		Properties props = new Properties();
//		KafkaLogger kafkaLogger = new KafkaLogger();
//		for (int i = 0; i < 10; i++) {
//			kafkaLogger.info("卧是一张dfgd dfg " + i, null, "xxdDDSSaas_e");
//			kafkaLogger.debug("debbbbbb" + i, null, null);
//			kafkaLogger.error("eeeeerrr" + i, null, null);
//		}
//	}

	public Level getEffectiveLevel() {
		return effectiveLevel;
	}

	public void setEffectiveLevel(Level effectiveLevel) {
		this.effectiveLevel = effectiveLevel;
	}

	public boolean isSystemOut() {
		return isSystemOut;
	}

	public void setSystemOut(boolean isSystemOut) {
		this.isSystemOut = isSystemOut;
	}

	public void setSystemOut(String isSystemOut) {
		boolean parseBoolean = this.isSystemOut;
		try {
			if (!StringUtils.isEmpty(isSystemOut)) {
				parseBoolean = Boolean.parseBoolean(isSystemOut);
			}
		} catch (Exception e) {
		}
		this.isSystemOut = parseBoolean;
	}

}
