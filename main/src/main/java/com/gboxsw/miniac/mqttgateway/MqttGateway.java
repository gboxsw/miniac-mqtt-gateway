package com.gboxsw.miniac.mqttgateway;

import java.util.*;
import java.util.logging.*;

import com.gboxsw.miniac.*;
import com.gboxsw.miniac.mqttutils.*;

import org.eclipse.paho.client.mqttv3.*;

public class MqttGateway extends Gateway {

	/**
	 * Logger.
	 */
	private static final Logger logger = Logger.getLogger(MqttGateway.class.getName());

	/**
	 * MQTT factory used to created MQTT instances.
	 */
	private final MqttFactory mqttFactory;

	/**
	 * MQTT client.
	 */
	private final IMqttAsyncClient mqttClient;

	/**
	 * MQTT connect options.
	 */
	private final MqttConnectOptions mqttConnectOptions;

	/**
	 * Creates MQTT gateway utilizing MQTT instances created by the provided
	 * MQTT factory.
	 * 
	 * @param mqttFactory
	 *            the MQTT factory.
	 */
	public MqttGateway(MqttFactory mqttFactory) {
		if (mqttFactory == null) {
			throw new NullPointerException("Mqtt factory cannot be null.");
		}

		this.mqttFactory = new MqttFactory(mqttFactory);
		this.mqttClient = mqttFactory.createAsyncClient();
		this.mqttConnectOptions = mqttFactory.createConnectOptions();

		configureGateway();
	}

	/**
	 * Creates MQTT gateway based on provided instances of MQTT client classes.
	 * 
	 * @param client
	 *            the asynchronous MQTT client.
	 * @param connectOptions
	 *            the connect options used to connect the client.
	 */
	public MqttGateway(IMqttAsyncClient client, MqttConnectOptions connectOptions) {
		if (client == null) {
			throw new NullPointerException("The client cannot be null.");
		}

		this.mqttFactory = null;
		this.mqttClient = client;
		this.mqttConnectOptions = connectOptions;

		configureGateway();
	}

	/**
	 * Realizes internal configuration of the gateway.
	 */
	private void configureGateway() {
		mqttClient.setCallback(new MqttCallbackExtended() {

			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				handleReceivedMessage(new Message(topic, message.getPayload()));
			}

			@Override
			public void deliveryComplete(IMqttDeliveryToken token) {
				// nothing to do
			}

			@Override
			public void connectionLost(Throwable cause) {
				handleReceivedMessage(new Message("$connected", "0"));
			}

			@Override
			public void connectComplete(boolean reconnect, String serverURI) {
				handleReceivedMessage(new Message("$connected", "1"));
			}
		});
	}

	@Override
	protected void onAddTopicFilter(String topicFilter) {
		try {
			logger.log(Level.INFO, "Subscribing to " + topicFilter);
			mqttClient.subscribe(topicFilter, 1);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Subscribing to " + topicFilter + " failed.", e);
		}
	}

	@Override
	protected void onRemoveTopicFilter(String topicFilter) {
		try {
			logger.log(Level.INFO, "Unsubscribing from " + topicFilter);
			mqttClient.unsubscribe(topicFilter);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unsubscribing from " + topicFilter + " failed.", e);
		}
	}

	@Override
	protected void onStart(Map<String, Bundle> bundles) {
		try {
			logger.log(Level.INFO, "Connecting to MQTT broker.");
			IMqttToken operationToken = (mqttConnectOptions != null) ? mqttClient.connect(mqttConnectOptions)
					: mqttClient.connect();
			operationToken.waitForCompletion();
			logger.log(Level.INFO, "Connected to MQTT broker.");
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Connection to MQTT broker failed.", e);
			throw new RuntimeException("Connection to MQTT broker failed.", e);
		}
	}

	@Override
	protected void onPublish(Message message) {
		try {
			boolean retained = false;
			String topic = message.getTopic();
			if (topic.endsWith("/#")) {
				topic = topic.substring(0, topic.length() - 2);
				retained = true;
			}
			MqttMessage mqttMessage = (mqttFactory != null) ? mqttFactory.createMessage(message.getPayload())
					: new MqttMessage(message.getPayload());
			mqttMessage.setRetained(retained);
			mqttClient.publish(topic, mqttMessage);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Publication of a MQTT message failed.", e);
		}
	}

	@Override
	protected void onSaveState(Map<String, Bundle> outBundles) {
		// nothing to do - the gateway does no need to save its state
	}

	@Override
	protected void onStop() {
		try {
			mqttClient.disconnect();
			mqttClient.close();
		} catch (MqttException e) {
			logger.log(Level.SEVERE, "Closing of MQTT client failed.", e);
		}
	}

	@Override
	protected boolean isValidTopicName(String topic) {
		if (topic == null) {
			return false;
		}

		if (topic.startsWith("$")) {
			return false;
		}

		if (topic.indexOf('+') >= 0) {
			return false;
		}

		// # is allowed only at the end of topic in the form /# as a flag
		// indicating a retained message
		int hashIdx = topic.indexOf('#');
		if ((hashIdx >= 0) && (hashIdx < topic.length() - 1)) {
			return false;
		}

		if (hashIdx == topic.length() - 1) {
			if (!topic.endsWith("/#")) {
				return false;
			}
		}

		return true;
	}
}
