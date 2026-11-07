/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/**
 * 将插桩状态按对象身份附着到 SDK 消息上，使状态随消息跨线程传递。
 *
 * <p>VirtualField 由 Agent 管理，不需要修改 SDK 的消息 API；这些字段只是进程内状态， 只有额外传播属性经过 GetPropertiesAdvice
 * 合并后才会写入消息并发送到远端。
 */
public class VirtualFieldStore {
  // 发送消息：保存构造时的调用方 Context，稍后由发送入口选为父上下文。
  private static final VirtualField<PublishingMessageImpl, Context> MESSAGE_CONTEXT =
      VirtualField.find(PublishingMessageImpl.class, Context.class);
  // 收到的消息：保存接收完成回调的环境 Context，交给监听器处理阶段使用。
  private static final VirtualField<MessageView, Context> MESSAGE_VIEW_CONTEXT =
      VirtualField.find(MessageView.class, Context.class);
  // 传播器写入的附加键值，与 SDK 原始业务属性分开保存，读取属性时再合并。
  private static final VirtualField<PublishingMessageImpl, Map<String, String>>
      MESSAGE_EXTRA_PROPERTIES = VirtualField.find(PublishingMessageImpl.class, Map.class);
  // namespace 来自发送候选队列，消费组来自接收协议请求；消息公共 API 无法直接提供这些信息。
  private static final VirtualField<PublishingMessageImpl, String> MESSAGE_NAMESPACE =
      VirtualField.find(PublishingMessageImpl.class, String.class);
  private static final VirtualField<MessageView, String> MESSAGE_CONSUMER_GROUP =
      VirtualField.find(MessageView.class, String.class);

  private VirtualFieldStore() {}

  @Nullable
  public static Context getContextByMessage(PublishingMessageImpl message) {
    return MESSAGE_CONTEXT.get(message);
  }

  @Nullable
  public static Context getContextByMessage(MessageView messageView) {
    return MESSAGE_VIEW_CONTEXT.get(messageView);
  }

  public static void setContextByMessage(PublishingMessageImpl message, Context context) {
    MESSAGE_CONTEXT.set(message, context);
  }

  public static void setContextByMessage(MessageView message, Context context) {
    MESSAGE_VIEW_CONTEXT.set(message, context);
  }

  @Nullable
  public static Map<String, String> getExtraPropertiesByMessage(PublishingMessageImpl message) {
    return MESSAGE_EXTRA_PROPERTIES.get(message);
  }

  // 在MessageMapSetter中被调用
  public static void setExtraPropertiesByMessage(
      PublishingMessageImpl message, Map<String, String> extraProperties) {
    MESSAGE_EXTRA_PROPERTIES.set(message, extraProperties);
  }

  @Nullable
  public static String getNamespaceByMessage(PublishingMessageImpl message) {
    return MESSAGE_NAMESPACE.get(message);
  }

  public static void setNamespaceByMessage(
      PublishingMessageImpl message, @Nullable String namespace) {
    MESSAGE_NAMESPACE.set(message, namespace);
  }

  @Nullable
  public static String getConsumerGroupByMessage(MessageView messageView) {
    return MESSAGE_CONSUMER_GROUP.get(messageView);
  }

  public static void setConsumerGroupByMessage(MessageView messageView, String consumerGroup) {
    MESSAGE_CONSUMER_GROUP.set(messageView, consumerGroup);
  }
}
