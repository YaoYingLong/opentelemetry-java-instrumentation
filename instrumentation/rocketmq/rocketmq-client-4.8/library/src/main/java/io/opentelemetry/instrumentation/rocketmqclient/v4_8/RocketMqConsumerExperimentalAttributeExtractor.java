/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.net.SocketAddress;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.common.message.MessageExt;

// 仅用于单条 Process，队列 ID、偏移和存储地址都取自这条 MessageExt。
// 批量 Process 使用另一个提取器，避免把第一条消息的信息误认为整个批次共有的信息。
final class RocketMqConsumerExperimentalAttributeExtractor
    implements AttributesExtractor<RocketMqConsumerRequest, ConsumeMessageContext> {

  // copied from MessagingIncubatingAttributes
  private static final AttributeKey<String> ROCKETMQ_MESSAGE_TAG =
      AttributeKey.stringKey("rocketmq.message.tag");

  private static final AttributeKey<Long> ROCKETMQ_QUEUE_ID =
      AttributeKey.longKey("rocketmq.queue_id");
  private static final AttributeKey<Long> ROCKETMQ_QUEUE_OFFSET =
      AttributeKey.longKey("rocketmq.queue_offset");
  private static final AttributeKey<String> ROCKETMQ_BROKER_ADDRESS =
      AttributeKey.stringKey("rocketmq.broker_address");

  @Override
  public void onStart(
      AttributesBuilder attributes, Context parentContext, RocketMqConsumerRequest request) {
    MessageExt msg = request.getMessage();
    attributes.put(ROCKETMQ_MESSAGE_TAG, msg.getTags());
    attributes.put(ROCKETMQ_QUEUE_ID, msg.getQueueId());
    attributes.put(ROCKETMQ_QUEUE_OFFSET, msg.getQueueOffset());
    // storeHost 是消息携带的 Broker 存储地址元数据，不表示本次消费连接的实际 socket 对端。
    SocketAddress storeHost = msg.getStoreHost();
    if (storeHost != null) {
      attributes.put(ROCKETMQ_BROKER_ADDRESS, getBrokerHost(storeHost));
    }
  }

  private static String getBrokerHost(SocketAddress storeHost) {
    // 保留现有字符串表示，只去掉 SocketAddress 格式中的斜杠；这里没有解析或拆分主机和端口。
    return storeHost.toString().replace("/", "");
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      RocketMqConsumerRequest request,
      @Nullable ConsumeMessageContext response,
      @Nullable Throwable error) {}
}
