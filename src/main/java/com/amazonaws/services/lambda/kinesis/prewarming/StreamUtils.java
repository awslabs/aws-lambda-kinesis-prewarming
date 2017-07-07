package com.amazonaws.services.lambda.kinesis.prewarming;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.scaling.ShardHashInfo;

public class StreamUtils {
	public static final int DESCRIBE_RETRIES = 10;
	// retry timeout set to 100ms as API's will potentially throttle > 10/sec
	public static final int RETRY_TIMEOUT_MS = 100;

	private static Object doOperation(AmazonKinesis kinesisClient, KinesisOperation operation, String streamName,
			int retries, boolean waitForActive) throws Exception {
		boolean done = false;
		int attempts = 0;
		Object result = null;
		do {
			attempts++;
			try {
				result = operation.run(kinesisClient);

				if (waitForActive) {
					waitForStreamStatus(kinesisClient, streamName, "ACTIVE");
				}
				done = true;
			} catch (ResourceInUseException e) {
				// thrown when the Shard is mutating - wait until we are able to
				// do the modification or ResourceNotFoundException is thrown
				Thread.sleep(1000);
			} catch (LimitExceededException lee) {
				// API Throttling
				Thread.sleep(getTimeoutDuration(attempts));
			}
		} while (!done && attempts < retries);

		if (!done) {
			throw new Exception(String.format("Unable to Complete Kinesis Operation after %s Retries", retries));
		} else {
			return result;
		}
	}

	// calculate an exponential backoff based on the attempt count
	private static final long getTimeoutDuration(int attemptCount) {
		return new Double(Math.pow(2, attemptCount) * RETRY_TIMEOUT_MS).longValue();
	}

	/**
	 * Wait for a Stream to become available or transition to the indicated
	 * status
	 * 
	 * @param streamName
	 * @param status
	 * @throws Exception
	 */
	public static void waitForStreamStatus(AmazonKinesis kinesisClient, String streamName, String status)
			throws Exception {
		boolean ok = false;
		String streamStatus;
		// stream mutation takes around 30 seconds, so we'll start with 20 as
		// a timeout
		int waitTimeout = 20000;
		do {
			streamStatus = getStreamStatus(kinesisClient, streamName);
			if (!streamStatus.equals(status)) {
				Thread.sleep(waitTimeout);
				// reduce the wait timeout from the initial wait time
				waitTimeout = 1000;
			} else {
				ok = true;
			}
		} while (!ok);
	}

	/**
	 * Get the status of a Stream
	 * 
	 * @param streamName
	 * @return
	 */
	protected static String getStreamStatus(AmazonKinesis kinesisClient, String streamName) throws Exception {
		return describeStream(kinesisClient, streamName, null).getStreamDescription().getStreamStatus();
	}

	private static interface KinesisOperation {
		public Object run(AmazonKinesis client);
	}

	public static DescribeStreamResult describeStream(final AmazonKinesis kinesisClient, final String streamName,
			final String shardIdStart) throws Exception {
		KinesisOperation describe = new KinesisOperation() {
			public Object run(AmazonKinesis client) {
				DescribeStreamResult result = client.describeStream(
						new DescribeStreamRequest().withStreamName(streamName).withExclusiveStartShardId(shardIdStart));

				return result;
			}
		};
		return (DescribeStreamResult) doOperation(kinesisClient, describe, streamName, DESCRIBE_RETRIES, false);

	}

	public static Map<String, ShardHashInfo> getOpenShards(AmazonKinesis kinesisClient, String streamName,
			String lastShardId) throws Exception {
		StreamDescription stream = null;
		Collection<String> openShardNames = new ArrayList<>();
		Map<String, ShardHashInfo> shardMap = new LinkedHashMap<>();

		// load all shards on the stream
		List<Shard> allShards = new ArrayList<>();

		do {
			stream = describeStream(kinesisClient, streamName, lastShardId).getStreamDescription();
			for (Shard shard : stream.getShards()) {
				allShards.add(shard);
				lastShardId = shard.getShardId();
			}
		} while (/* in some cases the describeStream call will return nothing */stream == null
				|| stream.getShards() == null || stream.getShards().size() == 0 || stream.getHasMoreShards());

		// load all the open shards on the Stream and sort if required
		for (Shard shard : allShards) {
			openShardNames.add(shard.getShardId());
			shardMap.put(shard.getShardId(), new ShardHashInfo(streamName, shard));

			// remove this Shard's parents from the set of active shards - they
			// are now closed and cannot be modified or written to
			if (shard.getParentShardId() != null) {
				openShardNames.remove(shard.getParentShardId());
				shardMap.remove(shard.getParentShardId());
			}
			if (shard.getAdjacentParentShardId() != null) {
				openShardNames.remove(shard.getAdjacentParentShardId());
				shardMap.remove(shard.getAdjacentParentShardId());
			}
		}

		// create a List of Open shards for sorting
		List<Shard> sortShards = new ArrayList<>();
		for (String s : openShardNames) {
			// paranoid null check in case we get a null map entry
			if (s != null) {
				sortShards.add(shardMap.get(s).getShard());
			}
		}

		// sort the list into lowest start hash order
		Collections.sort(sortShards, new Comparator<Shard>() {
			public int compare(Shard o1, Shard o2) {
				return compareShardsByStartHash(o1, o2);
			}
		});

		// build the Shard map into the correct order
		shardMap.clear();
		for (Shard s : sortShards) {
			shardMap.put(s.getShardId(), new ShardHashInfo(streamName, s));
		}

		return shardMap;
	}

	private static final int compareShardsByStartHash(Shard o1, Shard o2) {
		return new BigInteger(o1.getHashKeyRange().getStartingHashKey())
				.compareTo(new BigInteger(o2.getHashKeyRange().getStartingHashKey()));
	}
}
