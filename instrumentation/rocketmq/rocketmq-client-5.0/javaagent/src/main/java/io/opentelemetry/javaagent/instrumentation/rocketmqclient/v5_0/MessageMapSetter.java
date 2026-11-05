/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/**
 * 将传播器生成的键值暂存在消息的虚拟字段中，例如 W3C 传播器生成的 traceparent。
 *
 * <p>SDK 的 getProperties() 可能返回副本，直接修改一次读取结果无法保证后续序列化能看到。 配合 GetPropertiesAdvice
 * 在每次读取时合并附加属性，完成真正的消息传播。
 */
class MessageMapSetter implements TextMapSetter<PublishingMessageImpl> {

  @Override
  public void set(@Nullable PublishingMessageImpl message, String key, String value) {
    if (message == null) {
      return;
    }
    Map<String, String> extraProperties = VirtualFieldStore.getExtraPropertiesByMessage(message);
    if (extraProperties == null) {
      extraProperties = new HashMap<>();
      VirtualFieldStore.setExtraPropertiesByMessage(message, extraProperties);
    }
    extraProperties.put(key, value);
  }
}
