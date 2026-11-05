/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor;
import java.util.Objects;
import org.apache.rocketmq.common.message.MessageExt;

// 为一个批量 Process Span 关联每条消息的来源，并将批内不同的属性放在相应 Link 上。
final class RocketMqBatchProcessSpanLinksExtractor
    implements SpanLinksExtractor<RocketMqConsumerRequest> {

  // copied from MessagingIncubatingAttributes
  private static final AttributeKey<String> MESSAGING_DESTINATION_NAME =
      AttributeKey.stringKey("messaging.destination.name");
  private static final AttributeKey<String> MESSAGING_MESSAGE_ID =
      AttributeKey.stringKey("messaging.message.id");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  private final TextMapPropagator propagator;
  private final MessageExtractAdapter getter = new MessageExtractAdapter();
  private final boolean captureMessageTag;

  RocketMqBatchProcessSpanLinksExtractor(TextMapPropagator propagator, boolean captureMessageTag) {
    this.propagator = propagator;
    this.captureMessageTag = captureMessageTag;
  }

  @Override
  public void extract(
      SpanLinksBuilder spanLinks, Context parentContext, RocketMqConsumerRequest request) {
    for (MessageExt message : request.getMessages()) {
      // 使用 root 作为提取基底，确保消息缺少传播字段时不会误关联消费线程的当前 Span。
      Context extracted = propagator.extract(Context.root(), message, getter);
      AttributesBuilder attributes = Attributes.builder();
      // 批次公共值由 Span 属性提取器统一记录；仅将与公共值不同的 Topic、ID、Tag 写到单条 Link。
      // 此处没有聚合或复制任意消息 Header，也没有把第一条消息的队列信息套用到整批。
      if (!Objects.equals(message.getTopic(), request.getDestination())) {
        attributes.put(MESSAGING_DESTINATION_NAME, message.getTopic());
      }
      if (!Objects.equals(message.getMsgId(), request.getMessageId())) {
        attributes.put(MESSAGING_MESSAGE_ID, message.getMsgId());
      }
      if (captureMessageTag && !Objects.equals(message.getTags(), request.getMessageTag())) {
        attributes.put(ROCKETMQ_MESSAGE_TAG, message.getTags());
      }
      // 每条消息一个添加操作，不对同一来源去重；没有有效来源的消息不会生成有效 Link。
      spanLinks.addLink(Span.fromContext(extracted).getSpanContext(), attributes.build());
    }
  }
}
