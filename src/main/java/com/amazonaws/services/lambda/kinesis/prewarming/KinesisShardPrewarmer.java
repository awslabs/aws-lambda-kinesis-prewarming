package com.amazonaws.services.lambda.kinesis.prewarming;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.scaling.ShardHashInfo;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class KinesisShardPrewarmer {
	private Random r = new Random();

	private final byte[] randomBytes = new byte[16];

	public List<ShardPrewarmMessage> sendCanaryMessages(String streamName, String streamMessage,
			AmazonKinesis kinesisClient) throws Exception {
		Map<String, ShardHashInfo> openShards = StreamUtils.getOpenShards(kinesisClient, streamName, null);

		List<ShardPrewarmMessage> results = new ArrayList<>();

		// send a message to the stream on the start hash of each open shard
		openShards.keySet().forEach(shardId -> {
			// create a message to send based upon the provided stream message
			// this message will be set to use the explicit hash key for the
			// start of each shard, ensuring that we send a message to every
			// shard in the stream each time
			r.nextBytes(randomBytes);
			String thisPartitionKey = new String(randomBytes);
			String message = streamMessage == null ? new String() : streamMessage;
			Shard thisShard = openShards.get(shardId).getShard();
			String thisHashKey = thisShard.getHashKeyRange().getStartingHashKey();
			PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(streamName)
					.withData(ByteBuffer.wrap(message.getBytes())).withPartitionKey(thisPartitionKey)
					.withExplicitHashKey(thisHashKey);

			// write the message to kinesis
			PutRecordResult response = kinesisClient.putRecord(putRecordRequest);

			// add result information
			results.add(new ShardPrewarmMessage(shardId, thisPartitionKey, thisHashKey, message,
					response.getSequenceNumber()));
		});

		return results;
	}
}
