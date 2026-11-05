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
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;

/** 提供一次监听器处理对应的单条消息字段，并把消费返回值或异常转换成通用错误类型。 */
class RocketMqConsumerProcessAttributeGetter
    implements MessagingAttributesGetter<MessageView, ConsumeResult> {

  @Override
  public String getSystem(MessageView messageView) {
    return "rocketmq";
  }

  @Override
  public String getDestination(MessageView messageView) {
    return messageView.getTopic();
  }

  @Nullable
  @Override
  public String getDestinationTemplate(MessageView messageView) {
    return null;
  }

  @Override
  public boolean isTemporaryDestination(MessageView messageView) {
    return false;
  }

  @Override
  public boolean isAnonymousDestination(MessageView messageView) {
    return false;
  }

  @Nullable
  @Override
  public String getConversationId(MessageView messageView) {
    return null;
  }

  @Override
  public String getMessageId(MessageView messageView, @Nullable ConsumeResult unused) {
    return messageView.getMessageId().toString();
  }

  @Nullable
  @Override
  public String getClientId(MessageView messageView) {
    return null;
  }

  @Nullable
  @Override
  public Long getBatchMessageCount(MessageView messageView, @Nullable ConsumeResult unused) {
    return null;
  }

  @Nullable
  @Override
  public String getErrorType(
      MessageView messageView, @Nullable ConsumeResult consumeResult, @Nullable Throwable error) {
    // FAILURE 是业务回调明确报告的失败；抛异常时使用异常类名，与 Span 的错误状态相互配合。
    if (consumeResult == ConsumeResult.FAILURE) {
      return ConsumeResult.FAILURE.name();
    }
    return error == null ? null : error.getClass().getName();
  }

  @Override
  public List<String> getMessageHeader(MessageView messageView, String name) {
    String value = messageView.getProperties().get(name);
    if (value != null) {
      return singletonList(value);
    }
    return emptyList();
  }

  @Override
  public Collection<String> getMessageHeaderNames(MessageView messageView) {
    return messageView.getProperties().keySet();
  }
}
