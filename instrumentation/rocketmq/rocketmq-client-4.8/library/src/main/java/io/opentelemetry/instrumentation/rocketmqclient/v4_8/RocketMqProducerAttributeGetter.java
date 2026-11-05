/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientIDSetter;

// 读取 RocketMQ 发送请求，提供公共 messaging.* 属性所需的原始值；具体写入时机由公共提取器决定。
// namespace 和实验属性由另外的提取器负责，消息 body 不在这里采集。
final class RocketMqProducerAttributeGetter
    implements MessagingAttributesGetter<SendMessageContext, Void> {

  private final boolean messageCreation;

  RocketMqProducerAttributeGetter() {
    this(false);
  }

  RocketMqProducerAttributeGetter(boolean messageCreation) {
    // 同一个 Getter 服务 Create 和 Send；主要差异是消息 ID 的来源。
    this.messageCreation = messageCreation;
  }

  @Override
  public String getSystem(SendMessageContext request) {
    return "rocketmq";
  }

  @Nullable
  @Override
  public String getDestination(SendMessageContext request) {
    Message message = request.getMessage();
    if (message == null) {
      return null;
    }
    // 返回纯粹的topic，如果topic中带namespace%前缀会被截掉
    return RocketMqNamespaceUtil.withoutNamespace(
        message.getTopic(), RocketMqNamespaceUtil.getNamespace(request));
  }

  @Nullable
  @Override
  public String getDestinationTemplate(SendMessageContext request) {
    return null;
  }

  @Override
  public boolean isTemporaryDestination(SendMessageContext request) {
    return false;
  }

  @Override
  public boolean isAnonymousDestination(SendMessageContext request) {
    return false;
  }

  @Nullable
  @Override
  public String getConversationId(SendMessageContext request) {
    return null;
  }

  @Nullable
  @Override
  public String getMessageId(SendMessageContext request, @Nullable Void unused) {
    if (messageCreation) {
      // SDK 在批量 encode() 前已赋予消息唯一 ID，因此 Create 不需要等待 Broker 的发送结果。
      return MessageClientIDSetter.getUniqID(request.getMessage());
    }
    // the send result of a batch carries the concatenated ids of every message it contains, which
    // is not a per-message id, so it is not reported
    if (isBatch(request)) {
      return null;
    }
    SendResult sendResult = request.getSendResult();
    return sendResult == null ? null : sendResult.getMsgId();
  }

  @Nullable
  @Override
  public String getClientId(SendMessageContext request) {
    return null;
  }

  @Nullable
  @Override
  public Long getBatchMessageCount(SendMessageContext request, @Nullable Void unused) {
    if (!isBatch(request)) {
      return null;
    }
    if (request instanceof RocketMqProducerWrapper.BatchSendContext) {
      // library 代理使用独立 envelope，消息数量通过请求字段传递。
      return ((RocketMqProducerWrapper.BatchSendContext) request).messageCount;
    }
    if (RocketMqBatchSendSpanLinksExtractor.isBatchRequest(request)) {
      // javaagent helper 的上下文列表与原始消息逐条对应，数量不依赖 Span 是否被采样记录。
      return RocketMqBatchSendSpanLinksExtractor.getBatchMessageCount(request);
    }
    long batchSize = 0;
    for (Object ignored : (Iterable<?>) request.getMessage()) {
      batchSize++;
    }
    return batchSize;
  }

  private static boolean isBatch(SendMessageContext request) {
    return RocketMqBatchSendSpanLinksExtractor.isBatchRequest(request)
        || request instanceof RocketMqProducerWrapper.BatchSendContext
        || RocketMqMessageUtil.isBatch(request.getMessage());
  }

  @Override
  public List<String> getMessageHeader(SendMessageContext request, String name) {
    // 已登记的批量 Send 不合并逐条 Header；逐条 Create 或普通 Send 才从实际消息读取。
    if (RocketMqBatchSendSpanLinksExtractor.isBatchRequest(request)) {
      return emptyList();
    }
    Message message = request.getMessage();
    if (message == null) {
      return emptyList();
    }
    String value = message.getProperties().get(name);
    if (value != null) {
      // messaging.header.<name> 使用字符串数组属性，RocketMQ 的单值 property 也包装为单元素列表。
      return singletonList(value);
    }
    return emptyList();
  }

  @Override
  public Collection<String> getMessageHeaderNames(SendMessageContext request) {
    // 通配符及仅排除的选择器需要枚举属性名；精确指定的名称可直接通过 getMessageHeader 查询。
    if (RocketMqBatchSendSpanLinksExtractor.isBatchRequest(request)) {
      return emptyList();
    }
    Message message = request.getMessage();
    if (message == null) {
      return emptyList();
    }
    Map<String, String> properties = message.getProperties();
    return properties == null ? emptyList() : new ArrayList<>(properties.keySet());
  }
}
