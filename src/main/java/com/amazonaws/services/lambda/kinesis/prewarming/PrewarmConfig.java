package com.amazonaws.services.lambda.kinesis.prewarming;

import lombok.Getter;
import lombok.Setter;

public class PrewarmConfig {
	@Getter
	@Setter
	private String streamName = null;
	@Getter
	@Setter
	private String regionName = null;
	@Getter
	@Setter
	private String messagePrototype = null;

	public PrewarmConfig() {
	}

	public PrewarmConfig(String streamName, String regionName, String messagePrototype) {
		this.streamName = streamName;
		this.regionName = regionName;
		this.messagePrototype = messagePrototype;
	}

	protected Void validate() throws Exception {
		if (this.streamName == null) {
			throw new Exception("Configured Stream Name must not be null");
		}

		if (this.regionName == null) {
			throw new Exception("Configured Region must not be null");
		}

		return null;
	}

	@Override
	public String toString() {
		return String.format("%s,%s,%s", this.streamName, this.regionName, this.messagePrototype);
	}
}
