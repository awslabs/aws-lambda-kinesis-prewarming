package com.amazonaws.services.lambda.kinesis.prewarming;

import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ShardPrewarmerTests extends TestCase {
	private LambdaHandler handler = new LambdaHandler();

	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public ShardPrewarmerTests(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(ShardPrewarmerTests.class);
	}

	public void testHandler() throws Exception {
		PrewarmConfig config = new PrewarmConfig("IanTest", "eu-west-1", "Test Message");
		Integer resultCount = handler.handleRequest(config, null);

		assertEquals("Send Message Count", 3, resultCount.intValue());
	}

	public void testPrewarmer() throws Exception {
		// connect to kinesis and cache the connection
		AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard().withRegion("eu-west-1");
		AmazonKinesis useKinesisClient = builder.build();
		KinesisShardPrewarmer prewarmer = new KinesisShardPrewarmer();
		List<ShardPrewarmMessage> results = prewarmer.sendCanaryMessages("IanTest", "Test Message", useKinesisClient);
		assertEquals("Message Count OK - with Prototype", 3, results.size());

		results = prewarmer.sendCanaryMessages("IanTest", null, useKinesisClient);
		assertEquals("Message Count OK - no Prototype", 3, results.size());
	}

	public void testMultiRegion() throws Exception {
		// connect to kinesis and cache the connection
		AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard();
		builder.withRegion("eu-west-1");
		AmazonKinesis euWestClient = builder.build();
		builder.withRegion("us-east-1");
		AmazonKinesis usEastClient = builder.build();
		KinesisShardPrewarmer prewarmer = new KinesisShardPrewarmer();
		List<ShardPrewarmMessage> results = prewarmer.sendCanaryMessages("IanTest", "Test Message", euWestClient);
		assertNotSame("Message Count OK", 0, results.size());
		results = prewarmer.sendCanaryMessages("EnergyPipelineSensors", "Test Message", usEastClient);
		assertNotSame("Message Count OK", 0, results.size());
	}
}
