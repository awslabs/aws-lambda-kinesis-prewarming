package com.amazonaws.services.lambda.kinesis.prewarming;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class LambdaHandler implements RequestHandler<PrewarmConfig, Integer> {
	public static final String STREAM_NAME_PROPERTY = "StreamName";
	private Map<Region, AmazonKinesis> clientCache = new HashMap<>();
	private KinesisShardPrewarmer prewarmer = new KinesisShardPrewarmer();

	public Integer handleRequest(PrewarmConfig prewarmConfig, Context context) {
		try {
			prewarmConfig.validate();

			Region region = Region.getRegion(Regions.fromName(prewarmConfig.getRegionName()));
			AmazonKinesis useKinesisClient = null;

			// get the connection from the cache
			if (clientCache.containsKey(region)) {
				useKinesisClient = clientCache.get(region);
			} else {
				// connect to kinesis and cache the connection
				AmazonKinesisClientBuilder builder = AmazonKinesisClientBuilder.standard()
						.withRegion(prewarmConfig.getRegionName());
				useKinesisClient = builder.build();
				clientCache.put(region, useKinesisClient);
			}

			List<ShardPrewarmMessage> results = prewarmer.sendCanaryMessages(prewarmConfig.getStreamName(),
					prewarmConfig.getMessagePrototype(), useKinesisClient);
			results.forEach(item -> {
				System.out.println(
						String.format("Generated Kinesis canary message targeting Shard %s (Hash %s) sequence %s",
								item.getShardId(), item.getExplicitHashKey(), item.getSequenceNumber()));
			});

			return results.size();
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return -1;
		}
	}
}
