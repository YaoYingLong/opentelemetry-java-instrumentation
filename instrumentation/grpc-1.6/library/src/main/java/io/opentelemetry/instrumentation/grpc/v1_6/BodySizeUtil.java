/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import java.lang.reflect.Method;
import javax.annotation.Nullable;

/**
 * 可选的 protobuf 消息大小读取器。gRPC 可以使用其他消息编码，因此通过反射探测 MessageLite，避免强制依赖 protobuf。 结果是 protobuf 序列化大小，不包含
 * gRPC/HTTP2 帧开销，也不是压缩后的网络传输字节数。
 */
final class BodySizeUtil {
  @Nullable private static final Class<?> messageLiteClass = getMessageLiteClass();

  @Nullable
  private static final Method serializedSizeMethod =
      messageLiteClass != null ? getSerializedSizeMethod(messageLiteClass) : null;

  @Nullable
  private static Class<?> getMessageLiteClass() {
    try {
      return Class.forName("com.google.protobuf.MessageLite");
    } catch (Exception ignored) {
      return null;
    }
  }

  @Nullable
  private static Method getSerializedSizeMethod(Class<?> clazz) {
    try {
      return clazz.getMethod("getSerializedSize");
    } catch (NoSuchMethodException ignored) {
      return null;
    }
  }

  static <T> Long getBodySize(T message) {
    // 无 protobuf、消息不是 MessageLite 或反射调用失败时返回 null，表示大小未知，而非零字节。
    if (messageLiteClass == null || serializedSizeMethod == null) {
      return null;
    }
    if (!messageLiteClass.isInstance(message)) {
      return null;
    }
    try {
      return ((Integer) serializedSizeMethod.invoke(message)).longValue();
    } catch (Throwable ignored) {
      return null;
    }
  }

  private BodySizeUtil() {}
}
