package com.amazonaws.services.lambda.kinesis.prewarming;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ShardPrewarmMessage {
	@Getter
	private final String shardId;
	@Getter
	private final String partitionKey;
	@Getter
	private final String explicitHashKey;
	@Getter
	private final String message;
	@Getter
	private final String sequenceNumber;
}
