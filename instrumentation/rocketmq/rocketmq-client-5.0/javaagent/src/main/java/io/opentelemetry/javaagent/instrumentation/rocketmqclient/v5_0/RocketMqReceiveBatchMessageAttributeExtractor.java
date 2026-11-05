/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * 将整批消息一致的 RocketMQ 字段提升到 Receive Span。
 *
 * <p>各 Getter 已完成公共值判断；不一致或没有值时返回 null，AttributesBuilder 会忽略。 消息之间不同的字段留给
 * RocketMqReceiveSpanLinksExtractor 在各自的 Link 上记录。
 */
class RocketMqReceiveBatchMessageAttributeExtractor
    implements AttributesExtractor<RocketMqReceiveRequest, List<MessageView>> {

  // copied from semconv
  private static final AttributeKey<Long> ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP =
      AttributeKey.longKey("rocketmq.message.delivery_timestamp");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_GROUP =
      AttributeKey.stringKey("rocketmq.message.group");
  private static final AttributeKey<List<String>> ROCKETMQ_MESSAGE_KEYS =
      AttributeKey.stringArrayKey("rocketmq.message.keys");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, RocketMqReceiveRequest request) {
    attributes.put(ROCKETMQ_MESSAGE_TAG, request.getMessageTag());
    attributes.put(ROCKETMQ_MESSAGE_GROUP, request.getMessageGroup());
    attributes.put(ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, request.getMessageDeliveryTimestamp());
    attributes.put(ROCKETMQ_MESSAGE_KEYS, request.getMessageKeys());
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      RocketMqReceiveRequest request,
      @Nullable List<MessageView> messageViews,
      @Nullable Throwable error) {}
}
