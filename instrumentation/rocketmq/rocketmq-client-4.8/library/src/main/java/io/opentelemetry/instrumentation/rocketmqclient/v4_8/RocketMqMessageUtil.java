/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.Message;

// 为发送属性提取器识别 SDK 批次类型；使用类名保持对尚无 MessageBatch 的早期客户端版本的兼容。
final class RocketMqMessageUtil {

  static boolean isBatch(@Nullable Message message) {
    // Avoid linking MessageBatch because the oldest supported RocketMQ version predates that class.
    return message != null
        && message.getClass().getName().equals("org.apache.rocketmq.common.message.MessageBatch");
  }

  private RocketMqMessageUtil() {}
}
