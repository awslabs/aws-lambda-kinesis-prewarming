/**
 * Amazon Kinesis Scaling Utility
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.kinesis.scaling;

import java.math.BigInteger;

/**
 * AdjacentShards are a transfer object for maintaining references between an
 * open shard, and it's lower and higher neighbours by partition hash value
 */
public class AdjacentShards {
	private String streamName;

	private ShardHashInfo lowerShard;

	private ShardHashInfo higherShard;

	public AdjacentShards(String streamName, ShardHashInfo lower, ShardHashInfo higher) throws Exception {
		// ensure that the shards are adjacent
		if (!new BigInteger(higher.getShard().getHashKeyRange().getStartingHashKey())
				.subtract(new BigInteger(lower.getShard().getHashKeyRange().getEndingHashKey()))
				.equals(new BigInteger("1"))) {
			throw new Exception("Shards are not Adjacent");
		}
		this.streamName = streamName;
		this.lowerShard = lower;
		this.higherShard = higher;
	}

	protected ShardHashInfo getLowerShard() {
		return lowerShard;
	}

	protected ShardHashInfo getHigherShard() {
		return higherShard;
	}
}
