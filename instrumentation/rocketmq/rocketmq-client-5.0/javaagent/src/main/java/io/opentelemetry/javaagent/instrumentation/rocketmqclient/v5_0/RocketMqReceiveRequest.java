/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import apache.rocketmq.v2.ReceiveMessageRequest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.MessageViewImpl;

/**
 * 为两种接收入口提供统一的遥测请求视图，并按需计算一批消息的公共属性。
 *
 * <p>底层 ConsumerImpl 路径直接持有协议请求；SimpleConsumer 路径持有消费组及从返回消息恢复的 Topic/namespace。消息共有的值放在 Receive
 * Span 上，不同的值由 Link 提取器逐条记录。
 */
class RocketMqReceiveRequest {

  @Nullable private final ReceiveMessageRequest request;
  @Nullable private final String destination;
  @Nullable private final String namespace;
  @Nullable private final String consumerGroup;
  private final List<MessageView> messages;
  @Nullable private BatchMessageAttributes batchMessageAttributes;

  private RocketMqReceiveRequest(ReceiveMessageRequest request, List<MessageView> messages) {
    this.request = request;
    this.destination = null;
    this.namespace = null;
    this.consumerGroup = null;
    this.messages = messages;
  }

  private RocketMqReceiveRequest(
      @Nullable String destination,
      @Nullable String namespace,
      String consumerGroup,
      List<MessageView> messages) {
    this.request = null;
    this.destination = destination;
    this.namespace = namespace;
    this.consumerGroup = consumerGroup;
    this.messages = messages;
  }

  static RocketMqReceiveRequest create(ReceiveMessageRequest request, List<MessageView> messages) {
    return new RocketMqReceiveRequest(request, messages);
  }

  static RocketMqReceiveRequest create(String consumerGroup) {
    // SimpleConsumer 失败时尚未拿到消息，也没有暴露内部协议请求，因此不推测 Topic 或 namespace。
    return new RocketMqReceiveRequest(null, null, consumerGroup, emptyList());
  }

  static RocketMqReceiveRequest create(String consumerGroup, List<MessageView> messages) {
    // 调用方已过滤成功的空列表；SDK 在一次拉取中选择一个 Topic，第一条消息提供其 namespace。
    // 展示在 Span 上的 destination 仍由下面的公共值逻辑检查全部消息。
    MessageView message = messages.get(0);
    String namespace =
        StringUtils.emptyToNull(
            ((MessageViewImpl) message).getMessageQueue().getTopicResource().getNamespace());
    return new RocketMqReceiveRequest(message.getTopic(), namespace, consumerGroup, messages);
  }

  @Nullable
  String getRequestDestination() {
    // 保留请求目标供无消息的失败路径使用；有消息时，实际批次的 Topic 由 getDestination 返回。
    ReceiveMessageRequest request = this.request;
    return request == null ? destination : request.getMessageQueue().getTopic().getName();
  }

  @Nullable
  String getNamespace() {
    ReceiveMessageRequest request = this.request;
    return request == null
        ? namespace
        : StringUtils.emptyToNull(request.getMessageQueue().getTopic().getResourceNamespace());
  }

  String getConsumerGroup() {
    ReceiveMessageRequest request = this.request;
    return request == null ? requireNonNull(consumerGroup) : request.getGroup().getName();
  }

  List<MessageView> getMessages() {
    return messages;
  }

  @Nullable
  String getDestination() {
    return getBatchMessageAttributes().destination;
  }

  @Nullable
  String getMessageTag() {
    return getBatchMessageAttributes().messageTag;
  }

  @Nullable
  String getMessageGroup() {
    return getBatchMessageAttributes().messageGroup;
  }

  @Nullable
  Long getMessageDeliveryTimestamp() {
    return getBatchMessageAttributes().messageDeliveryTimestamp;
  }

  @Nullable
  List<String> getMessageKeys() {
    return getBatchMessageAttributes().messageKeys;
  }

  private BatchMessageAttributes getBatchMessageAttributes() {
    // 延迟到属性或 Link 真正需要时再遍历消息；关闭遥测或被抑制的接收无需做整批属性比较。
    // 一个请求的多个 Getter 共用这次计算结果，避免在逐条提取 Link 时重复扫描整批消息。
    BatchMessageAttributes attributes = batchMessageAttributes;
    if (attributes == null) {
      attributes =
          new BatchMessageAttributes(messages.isEmpty() ? getRequestDestination() : null, messages);
      batchMessageAttributes = attributes;
    }
    return attributes;
  }

  @Nullable
  private static <T> T commonValue(
      List<MessageView> messages, Function<MessageView, T> valueExtractor) {
    // 只有所有消息值都相等才提升为批次属性；null 表示未提供公共值，不能用首条代表整批。
    if (messages.isEmpty()) {
      return null;
    }
    T value = valueExtractor.apply(messages.get(0));
    for (int i = 1; i < messages.size(); i++) {
      if (!Objects.equals(value, valueExtractor.apply(messages.get(i)))) {
        return null;
      }
    }
    return value;
  }

  @Nullable
  private static List<String> commonKeys(List<MessageView> messages) {
    if (messages.isEmpty()) {
      return null;
    }
    List<String> keys = new ArrayList<>(messages.get(0).getKeys());
    // keys 按集合比较：顺序不影响是否相同；相同时保留第一条消息的列表用于输出。
    HashSet<String> keySet = new HashSet<>(keys);
    for (int i = 1; i < messages.size(); i++) {
      if (!keySet.equals(new HashSet<>(messages.get(i).getKeys()))) {
        return null;
      }
    }
    return keys;
  }

  private static class BatchMessageAttributes {
    @Nullable private final String destination;
    @Nullable private final String messageTag;
    @Nullable private final String messageGroup;
    @Nullable private final Long messageDeliveryTimestamp;
    @Nullable private final List<String> messageKeys;

    private BatchMessageAttributes(
        @Nullable String requestDestination, List<MessageView> messages) {
      destination =
          messages.isEmpty() ? requestDestination : commonValue(messages, MessageView::getTopic);
      messageTag = commonValue(messages, message -> message.getTag().orElse(null));
      messageGroup = commonValue(messages, message -> message.getMessageGroup().orElse(null));
      messageDeliveryTimestamp =
          commonValue(messages, message -> message.getDeliveryTimestamp().orElse(null));
      messageKeys = commonKeys(messages);
    }
  }
}
