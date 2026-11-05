/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.semconv.incubating.RocketmqIncubatingAttributes.RocketmqMessageTypeIncubatingValues;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/** 补充发送消息的 RocketMQ 专有属性；数据在发送开始时已可获得，结束时无需从回执重复提取。 */
class RocketMqProducerAttributeExtractor
    implements AttributesExtractor<PublishingMessageImpl, SendReceiptImpl> {
  // copied from semconv
  private static final AttributeKey<Long> ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP =
      AttributeKey.longKey("rocketmq.message.delivery_timestamp");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_GROUP =
      AttributeKey.stringKey("rocketmq.message.group");
  private static final AttributeKey<List<String>> ROCKETMQ_MESSAGE_KEYS =
      AttributeKey.stringArrayKey("rocketmq.message.keys");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TYPE =
      AttributeKey.stringKey("rocketmq.message.type");
  private static final AttributeKey<String> ROCKETMQ_NAMESPACE =
      AttributeKey.stringKey("rocketmq.namespace");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, PublishingMessageImpl message) {
    attributes.put(ROCKETMQ_NAMESPACE, VirtualFieldStore.getNamespaceByMessage(message));
    // tag 用于消息过滤，message group 表示 FIFO 分组，delivery timestamp 是定时投递时间。
    message.getTag().ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_TAG, s));
    message.getMessageGroup().ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_GROUP, s));
    message
        .getDeliveryTimestamp()
        .ifPresent(s -> attributes.put(ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, s));
    attributes.put(ROCKETMQ_MESSAGE_KEYS, new ArrayList<>(message.getKeys()));
    // 当前映射显式区分 FIFO、DELAY、TRANSACTION，其余枚举值统一记录为 NORMAL。
    switch (message.getMessageType()) {
      case FIFO:
        attributes.put(ROCKETMQ_MESSAGE_TYPE, RocketmqMessageTypeIncubatingValues.FIFO);
        break;
      case DELAY:
        attributes.put(ROCKETMQ_MESSAGE_TYPE, RocketmqMessageTypeIncubatingValues.DELAY);
        break;
      case TRANSACTION:
        attributes.put(ROCKETMQ_MESSAGE_TYPE, RocketmqMessageTypeIncubatingValues.TRANSACTION);
        break;
      default:
        attributes.put(ROCKETMQ_MESSAGE_TYPE, RocketmqMessageTypeIncubatingValues.NORMAL);
    }
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      PublishingMessageImpl message,
      @Nullable SendReceiptImpl sendReceipt,
      @Nullable Throwable error) {}
}
