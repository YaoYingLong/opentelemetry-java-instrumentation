/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;

// 实验属性开启时，只把所有消息一致的 Tag 放在批量 Process 上；不同 Tag 由逐条 Link 携带。
final class RocketMqBatchProcessAttributeExtractor
    implements AttributesExtractor<RocketMqConsumerRequest, ConsumeMessageContext> {

  // copied from MessagingIncubatingAttributes
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, RocketMqConsumerRequest request) {
    attributes.put(ROCKETMQ_MESSAGE_TAG, request.getMessageTag());
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      RocketMqConsumerRequest request,
      @Nullable ConsumeMessageContext response,
      @Nullable Throwable error) {}
}
