/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_CONSUMER_GROUP_NAME;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageViewImpl;

/** 为单条消息的 Process Span 补充 RocketMQ 字段；这里无需做 Receive 批次的公共值合并。 */
class RocketMqConsumerProcessAttributeExtractor
    implements AttributesExtractor<MessageView, ConsumeResult> {

  // copied from semconv
  private static final AttributeKey<Long> ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP =
      AttributeKey.longKey("rocketmq.message.delivery_timestamp");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_GROUP =
      AttributeKey.stringKey("rocketmq.message.group");
  private static final AttributeKey<List<String>> ROCKETMQ_MESSAGE_KEYS =
      AttributeKey.stringArrayKey("rocketmq.message.keys");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");
  private static final AttributeKey<String> ROCKETMQ_NAMESPACE =
      AttributeKey.stringKey("rocketmq.namespace");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, MessageView messageView) {
    messageView.getTag().ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_TAG, s));
    messageView.getMessageGroup().ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_GROUP, s));
    messageView
        .getDeliveryTimestamp()
        .ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, s));
    attributes.put(ROCKETMQ_MESSAGE_KEYS, new ArrayList<>(messageView.getKeys()));
    // 接收回调已从协议请求中取出消费组，并随 MessageView 一起传递到当前处理线程。
    String consumerGroup = VirtualFieldStore.getConsumerGroupByMessage(messageView);
    attributes.put(MESSAGING_CONSUMER_GROUP_NAME, consumerGroup);
    // namespace 来自消息所关联队列的 Topic Resource，空字符串统一视为未提供。
    String namespace =
        ((MessageViewImpl) messageView).getMessageQueue().getTopicResource().getNamespace();
    attributes.put(ROCKETMQ_NAMESPACE, StringUtils.emptyToNull(namespace));
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      MessageView messageView,
      @Nullable ConsumeResult consumeResult,
      @Nullable Throwable error) {}
}
