
package com.primus.gcp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class MosquittoSubscriber implements MqttCallback {
	
	@Autowired
	private GCPPublishAndSubscribe publisher;

	private final int qos = 1;
	private MqttClient client;
	private RestTemplate restTemplate = new RestTemplate();
	private ExecutorService executor = Executors.newCachedThreadPool();
	private String topic = "telemetry";
	private final String uri = "http://localhost:8081/publishGCP";

	public MosquittoSubscriber() throws MqttException {
		String host = String.format("tcp://%s:%d", "35.224.63.37", 1883);
		String username = "username";
		String password = "password";
		String clientId = "JaceToMosquitto";

		MqttConnectOptions conOpt = new MqttConnectOptions();
		conOpt.setCleanSession(true);
		conOpt.setUserName(username);
		conOpt.setPassword(password.toCharArray());

		this.client = new MqttClient(host, clientId);
		this.client.setCallback(this);
		this.client.connect(conOpt);
		this.client.subscribe(this.topic, qos);
		System.out.println("Middleware have successfully subscribed to Jace 8000 Telemetry!");
	}

	public void messageArrived(String topic, MqttMessage message) throws MqttException {
		System.out.println("Mosquitto Receive Message!" + message.getPayload());
		publisher.publishGCP(message.getPayload());
//		CompletableFuture.supplyAsync(() -> {
//			restTemplate.postForEntity(uri, message.getPayload(), String.class);
//			return "Done";
//		}, executor);
	}

	public void connectionLost(Throwable cause) {
		System.out.println("Connection lost because: " + cause);
		System.exit(1);
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
	}
}
