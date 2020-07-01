package com.primus.gcp;

import java.io.File;
import java.io.FileInputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import Model.DeviceInfo;

@Controller
public class GCPPublishAndSubscribe {
	private Map<String, MqttClient> map = new HashMap<>();
	private List<DeviceInfo> deviceInfoList = new ArrayList<>();

	public GCPPublishAndSubscribe() {
		try {
			File file = ResourceUtils.getFile("classpath:eqp.xlsx");
			FileInputStream fis = new FileInputStream(file);
			XSSFWorkbook wb = new XSSFWorkbook(fis);
			XSSFSheet sh = wb.getSheet("eqp");
			DataFormatter formatter = new DataFormatter();

			MqttExampleOptions options = new MqttExampleOptions();
			for (int i = 1; i < sh.getLastRowNum() + 1; i++) {
				String deviceId = formatter.formatCellValue(sh.getRow(i).getCell(0));
				String privateKeyFile = formatter.formatCellValue(sh.getRow(i).getCell(1));
				options.setDeviceId(deviceId);
				options.setPrivateKey(privateKeyFile);
				MqttClient mqttClient = MqttExample.getMqttClient(options);
				map.put(deviceId, mqttClient);

				DeviceInfo deviceInfo = new DeviceInfo();
				deviceInfo.setDeviceId(deviceId);
				deviceInfo.setPrivateKey(privateKeyFile);
				deviceInfoList.add(deviceInfo);
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

//	public void publishGCP(byte[] bytes) {
//		try {
//			System.out.println("Received Msg from Mosquitto!");
//			String jsonStr = new String(bytes);
//			System.out.println(jsonStr);
//			System.out.println("Start GCP Publishing");
//			JSONObject jsonObj = new JSONObject(jsonStr);
//			String deviceId = String.valueOf(jsonObj.get("deviceId"));
//			jsonObj.remove("deviceId");
//
//			MqttClient mqttClient = map.get(deviceId);
//			MqttMessage mqttMessage = new MqttMessage();
//			mqttMessage.setPayload(jsonObj.toString().getBytes());
//			String mqttTopic = String.format("/devices/%s/events/%s", deviceId, "iot");
//			mqttClient.publish(mqttTopic, mqttMessage);
//			System.out.println("GCP Pub/Sub Updated!");
//		} catch (Exception e) {
//			System.out.println(e.getMessage());
//		}
//	}
	
	@Bean
	public Instant getTimeStamp() {
		 Clock offsetClock = Clock.offset(Clock.systemUTC(), Duration.ofHours(+8));
		 return Instant.now(offsetClock);
	}
	
	public void publishGCP(byte[] bytes) {
		try {
			System.out.println("Received Msg from Mosquitto!");
			String jsonStr = new String(bytes);
			JSONObject json = new JSONObject();
			JSONObject json2 = new JSONObject();
			json.put("version", 1);
			json.put("timestamp", getTimeStamp());
			json.put("points", json2);
			JSONObject json3 = new JSONObject();
			json2.put("run_status", json3);
			json3.put("present_value", jsonStr);
			System.out.println(json.toString());
			System.out.println("Start GCP Publishing");

			MqttClient mqttClient = map.get("FCU-19");
			MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setPayload(json.toString().getBytes());
			String mqttTopic = String.format("/devices/%s/events/%s", "FCU-19", "iot");
			mqttClient.publish(mqttTopic, mqttMessage);
			System.out.println("GCP Pub/Sub Updated!");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	@Scheduled(cron = "0 * * * * *")
	public void doScheduledWork() {
		MqttExampleOptions options = new MqttExampleOptions();
		deviceInfoList.forEach(x -> {
			try {
				String deviceId = x.getDeviceId();
				String privateKeyFile = x.getPrivateKey();
				options.setDeviceId(deviceId);
				options.setPrivateKey(privateKeyFile);
				MqttClient mqttClient = MqttExample.getMqttClient(options);
				map.put(deviceId, mqttClient);
				System.out.println("JWT Token Renewed for : "+deviceId);
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		});
	}
}
