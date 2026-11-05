/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.SendMessageContext;

// 按开关补充 RocketMQ 专有属性，和公共 messaging.* 属性及 Header 选择器分别配置。
// 当前地址来自 SDK 的 brokerAddr，并未拆分成 server.*，也不是 socket 层的实际对端观测。
final class RocketMqProducerExperimentalAttributeExtractor
    implements AttributesExtractor<SendMessageContext, Void> {

  // copied from MessagingIncubatingAttributes
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  private static final AttributeKey<String> ROCKETMQ_BROKER_ADDRESS =
      AttributeKey.stringKey("rocketmq.broker_address");
  private static final AttributeKey<String> ROCKETMQ_SEND_RESULT =
      AttributeKey.stringKey("rocketmq.send_result");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, SendMessageContext request) {
    // 普通消息可记录 Tag；已登记的 javaagent 批量 Send 没有单条消息 Tag，不取第一条代替整批。
    if (request.getMessage() != null
        && !RocketMqBatchSendSpanLinksExtractor.isBatchRequest(request)) {
      attributes.put(ROCKETMQ_MESSAGE_TAG, request.getMessage().getTags());
    }
    attributes.put(ROCKETMQ_BROKER_ADDRESS, request.getBrokerAddr());
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      SendMessageContext request,
      @Nullable Void unused,
      @Nullable Throwable error) {
    // 批量 Send 在 encode() 前开始，当时尚未选择 Broker；Hook 回填后需在结束时再次读取。
    attributes.put(ROCKETMQ_BROKER_ADDRESS, request.getBrokerAddr());
    if (request.getSendResult() != null) {
      // 发送状态字符串只作为属性记录；是否标记 Span ERROR 由 Instrumenter 的异常/状态规则负责。
      attributes.put(ROCKETMQ_SEND_RESULT, request.getSendResult().getSendStatus().name());
    }
  }
}
