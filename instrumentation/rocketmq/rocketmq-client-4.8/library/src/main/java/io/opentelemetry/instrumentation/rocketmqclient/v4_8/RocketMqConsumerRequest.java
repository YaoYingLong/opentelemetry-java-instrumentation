/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import static java.util.Collections.singletonList;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.MessageExt;

// 将 SDK 的消费消息列表转换成 Instrumenter 请求，统一承载单条与批量消费。
// 单条属性直接来自消息；批次属性只有在所有消息一致时才保留，差异交给逐条 Span Link 表达。
final class RocketMqConsumerRequest {

  // 第一条消息供单条消费提取器使用；批量属性必须走下方的 commonValue，不能直接套用第一条。
  private final MessageExt message;
  private final List<MessageExt> messages;
  private final String consumerGroup;
  private final int batchSize;
  @Nullable private final String namespace;
  // null 也可能是已经计算过的结果，用独立标记区分“尚未计算”和“批内不一致”。
  private boolean destinationInitialized;
  @Nullable private String destination;
  private boolean messageIdInitialized;
  @Nullable private String messageId;
  private boolean messageTagInitialized;
  @Nullable private String messageTag;

  RocketMqConsumerRequest(
      MessageExt message, String consumerGroup, int batchSize, @Nullable String namespace) {
    this(message, singletonList(message), consumerGroup, batchSize, namespace);
  }

  RocketMqConsumerRequest(
      List<MessageExt> messages, String consumerGroup, int batchSize, @Nullable String namespace) {
    this(messages.get(0), messages, consumerGroup, batchSize, namespace);
  }

  private RocketMqConsumerRequest(
      MessageExt message,
      List<MessageExt> messages,
      String consumerGroup,
      int batchSize,
      @Nullable String namespace) {
    this.message = message;
    this.messages = messages;
    // namespace 单独作为属性记录，消费组属性保留去掉 namespace 后的业务名称。
    this.consumerGroup = RocketMqNamespaceUtil.withoutNamespace(consumerGroup, namespace);
    this.batchSize = batchSize;
    this.namespace = namespace;
  }

  MessageExt getMessage() {
    return message;
  }

  List<MessageExt> getMessages() {
    return messages;
  }

  /** Returns whether this request accounts for more than one message. */
  boolean isBatch() {
    return messages.size() > 1;
  }

  String getConsumerGroup() {
    return consumerGroup;
  }

  int getBatchSize() {
    return batchSize;
  }

  @Nullable
  String getNamespace() {
    return namespace;
  }

  @Nullable
  String getDestination() {
    if (!destinationInitialized) {
      destination = commonValue(messages, MessageExt::getTopic);
      destinationInitialized = true;
    }
    return destination;
  }

  @Nullable
  String getMessageId() {
    if (!messageIdInitialized) {
      messageId = commonValue(messages, MessageExt::getMsgId);
      messageIdInitialized = true;
    }
    return messageId;
  }

  @Nullable
  String getMessageTag() {
    if (!messageTagInitialized) {
      messageTag = commonValue(messages, MessageExt::getTags);
      messageTagInitialized = true;
    }
    return messageTag;
  }

  @Nullable
  private static String commonValue(
      List<MessageExt> messages, Function<MessageExt, String> valueExtractor) {
    // 当前请求由非空消息列表构造，找到任意差异即可返回 null，让属性提取器省略批次公共值。
    // Objects.equals 同时处理消息属性为 null 的情况；所有值均为 null 时公共值也为 null。
    Iterator<MessageExt> iterator = messages.iterator();
    String value = valueExtractor.apply(iterator.next());
    while (iterator.hasNext()) {
      if (!Objects.equals(value, valueExtractor.apply(iterator.next()))) {
        return null;
      }
    }
    return value;
  }
}
