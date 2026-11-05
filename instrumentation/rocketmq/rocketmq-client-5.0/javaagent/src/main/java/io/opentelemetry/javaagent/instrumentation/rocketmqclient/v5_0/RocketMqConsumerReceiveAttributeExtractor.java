/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_CONSUMER_GROUP_NAME;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;

/** 从统一接收请求中提取消费组和 namespace，兼容协议请求与 SimpleConsumer 返回消息两种来源。 */
class RocketMqConsumerReceiveAttributeExtractor
    implements AttributesExtractor<RocketMqReceiveRequest, List<MessageView>> {

  // copied from semconv
  private static final AttributeKey<String> ROCKETMQ_NAMESPACE =
      AttributeKey.stringKey("rocketmq.namespace");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, RocketMqReceiveRequest request) {
    String consumerGroup = request.getConsumerGroup();
    attributes.put(MESSAGING_CONSUMER_GROUP_NAME, consumerGroup);
    // 失败时可能没有 namespace；AttributesBuilder 忽略 null，不会输出空占位属性。
    attributes.put(ROCKETMQ_NAMESPACE, request.getNamespace());
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      RocketMqReceiveRequest request,
      @Nullable List<MessageView> messageViews,
      @Nullable Throwable error) {}
}
