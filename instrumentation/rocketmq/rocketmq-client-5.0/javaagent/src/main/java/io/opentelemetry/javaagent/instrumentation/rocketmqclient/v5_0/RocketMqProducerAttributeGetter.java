/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/** 提供单条发送消息的通用字段，由 Messaging 提取器生成名称、消息属性及选中的消息头属性。 */
class RocketMqProducerAttributeGetter
    implements MessagingAttributesGetter<PublishingMessageImpl, SendReceiptImpl> {

  @Override
  public String getSystem(PublishingMessageImpl message) {
    return "rocketmq";
  }

  @Override
  public String getDestination(PublishingMessageImpl message) {
    return message.getTopic();
  }

  @Nullable
  @Override
  public String getDestinationTemplate(PublishingMessageImpl message) {
    return null;
  }

  @Override
  public boolean isTemporaryDestination(PublishingMessageImpl message) {
    return false;
  }

  @Override
  public boolean isAnonymousDestination(PublishingMessageImpl message) {
    return false;
  }

  @Nullable
  @Override
  public String getConversationId(PublishingMessageImpl message) {
    return null;
  }

  @Override
  public String getMessageId(PublishingMessageImpl message, @Nullable SendReceiptImpl sendReceipt) {
    // 待发送消息已经持有 SDK 分配的 ID，即使没有成功回执也能取得。
    return message.getMessageId().toString();
  }

  @Nullable
  @Override
  public String getClientId(PublishingMessageImpl message) {
    return null;
  }

  @Nullable
  @Override
  public Long getBatchMessageCount(
      PublishingMessageImpl publishingMessage, @Nullable SendReceiptImpl sendReceipt) {
    // 底层虽然使用消息列表发送，但本模块为每条消息分别创建 Send Span，因此这里不报告批次数量。
    return null;
  }

  @Override
  public List<String> getMessageHeader(PublishingMessageImpl message, String name) {
    String value = message.getProperties().get(name);
    if (value != null) {
      return singletonList(value);
    }
    return emptyList();
  }

  @Override
  public Collection<String> getMessageHeaderNames(PublishingMessageImpl message) {
    return message.getProperties().keySet();
  }
}
