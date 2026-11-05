/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static java.util.Collections.emptyList;

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;

/** 提供整个 Receive 批次的通用属性；逐条消息的身份和差异由 Link 表达。 */
class RocketMqConsumerReceiveAttributeGetter
    implements MessagingAttributesGetter<RocketMqReceiveRequest, List<MessageView>> {

  @Override
  public String getSystem(RocketMqReceiveRequest request) {
    return "rocketmq";
  }

  @Override
  @Nullable
  public String getDestination(RocketMqReceiveRequest request) {
    return request.getDestination();
  }

  @Nullable
  @Override
  public String getDestinationTemplate(RocketMqReceiveRequest request) {
    return null;
  }

  @Override
  public boolean isTemporaryDestination(RocketMqReceiveRequest request) {
    return false;
  }

  @Override
  public boolean isAnonymousDestination(RocketMqReceiveRequest request) {
    return false;
  }

  @Nullable
  @Override
  public String getConversationId(RocketMqReceiveRequest request) {
    return null;
  }

  @Nullable
  @Override
  public String getMessageId(RocketMqReceiveRequest request, @Nullable List<MessageView> unused) {
    // Receive 始终按批次建模，即便只收到一条消息，消息 ID 也放在该消息的 Link 上。
    return null;
  }

  @Nullable
  @Override
  public String getClientId(RocketMqReceiveRequest request) {
    return null;
  }

  @Nullable
  @Override
  public Long getBatchMessageCount(
      RocketMqReceiveRequest request, @Nullable List<MessageView> messages) {
    // 成功时记录实际消息数；失败响应为 null，不能把失败报告为成功收到 0 条消息。
    return messages != null ? (long) messages.size() : null;
  }

  @Override
  public Collection<String> getMessageHeaderNames(RocketMqReceiveRequest request) {
    // 批次中的消息头可能不同，当前实现不在 Receive Span 上采集消息头。
    // Link 提取器另行记录消息 ID 和 RocketMQ 字段，并未自动采集任意用户消息头。
    return emptyList();
  }
}
