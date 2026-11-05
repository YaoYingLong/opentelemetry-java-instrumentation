/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_MESSAGE_ID;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * 为接收批次中的各条消息提交指向其生产端上下文的 Link，并携带消息级属性。
 *
 * <p>Receive Span 可以关联多个生产端 Span；消息 ID 始终属于对应 Link，批次共有的其他字段 已由 Span
 * 属性提取器记录，这里仅补充各条消息的差异，避免重复存储相同信息。
 */
class RocketMqReceiveSpanLinksExtractor implements SpanLinksExtractor<RocketMqReceiveRequest> {
  // copied from semconv
  private static final AttributeKey<Long> ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP =
      AttributeKey.longKey("rocketmq.message.delivery_timestamp");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_GROUP =
      AttributeKey.stringKey("rocketmq.message.group");
  private static final AttributeKey<List<String>> ROCKETMQ_MESSAGE_KEYS =
      AttributeKey.stringArrayKey("rocketmq.message.keys");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  private final TextMapPropagator propagator;
  private final MessageMapGetter getter = new MessageMapGetter();

  RocketMqReceiveSpanLinksExtractor(TextMapPropagator propagator) {
    this.propagator = propagator;
  }

  @Override
  public void extract(
      SpanLinksBuilder spanLinks, Context parentContext, RocketMqReceiveRequest request) {
    for (MessageView message : request.getMessages()) {
      // Link 只能反映消息本身携带的上下文，因此以 root 为提取起点；若使用 parentContext，
      // 缺少传播字段的消息可能错误地关联到消费者当前的 Span。
      Context extracted = propagator.extract(Context.root(), message, getter);
      AttributesBuilder attributes = Attributes.builder();
      if (!Objects.equals(message.getTopic(), request.getDestination())) {
        attributes.put(MESSAGING_DESTINATION_NAME, message.getTopic());
      }
      String messageId = Objects.toString(message.getMessageId(), null);
      attributes.put(MESSAGING_MESSAGE_ID, messageId);
      String messageTag = message.getTag().orElse(null);
      if (!Objects.equals(messageTag, request.getMessageTag())) {
        attributes.put(ROCKETMQ_MESSAGE_TAG, messageTag);
      }
      String messageGroup = message.getMessageGroup().orElse(null);
      if (!Objects.equals(messageGroup, request.getMessageGroup())) {
        attributes.put(ROCKETMQ_MESSAGE_GROUP, messageGroup);
      }
      Long deliveryTimestamp = message.getDeliveryTimestamp().orElse(null);
      if (!Objects.equals(deliveryTimestamp, request.getMessageDeliveryTimestamp())) {
        attributes.put(ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, deliveryTimestamp);
      }
      ArrayList<String> messageKeys = new ArrayList<>(message.getKeys());
      if (request.getMessageKeys() == null
          || !new HashSet<>(messageKeys).equals(new HashSet<>(request.getMessageKeys()))) {
        attributes.put(ROCKETMQ_MESSAGE_KEYS, messageKeys);
      }
      // 是否接受该 SpanContext 由底层 SpanBuilder 决定；没有有效传播上下文时不会凭空创建生产端 Span。
      spanLinks.addLink(Span.fromContext(extracted).getSpanContext(), attributes.build());
    }
  }
}
