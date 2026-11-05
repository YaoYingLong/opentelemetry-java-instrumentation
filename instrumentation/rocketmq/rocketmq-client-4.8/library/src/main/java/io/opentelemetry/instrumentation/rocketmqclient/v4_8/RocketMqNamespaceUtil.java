/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import java.lang.reflect.Method;
import javax.annotation.Nullable;

// 将 namespace 作为独立遥测属性保留，并从 Topic/消费组名称中移除对应前缀。
// 通过反射兼容没有 getNamespace() 的早期 SDK，不把新版方法变成所有版本都必须具备的链接依赖。
final class RocketMqNamespaceUtil {

  private static final NamespaceAccessor noNamespaceAccessor = context -> null;
  // 按实际类型缓存访问器，包含“无此方法”的结果，避免每条消息重复查找反射方法。
  private static final ClassValue<NamespaceAccessor> namespaceAccessors =
      new ClassValue<NamespaceAccessor>() {
        @Override
        protected NamespaceAccessor computeValue(Class<?> type) {
          try {
            Method method = type.getMethod("getNamespace");
            return context -> invoke(method, context);
          } catch (NoSuchMethodException ignored) {
            return noNamespaceAccessor;
          }
        }
      };

  @Nullable
  static String getNamespace(Object context) {
    return namespaceAccessors.get(context.getClass()).get(context);
  }

  @Nullable
  private static String invoke(Method method, Object context) {
    try {
      return (String) method.invoke(context);
    } catch (ReflectiveOperationException ignored) {
      return null;
    }
  }

  static String withoutNamespace(String resource, @Nullable String namespace) {
    // resource 来自生产端 Topic 或消费请求中的 consumerGroup，不局限于 Topic。
    if (resource.isEmpty() || namespace == null || namespace.isEmpty()) {
      return resource;
    }

    String prefix = "";
    String resourceWithoutPrefix = resource;
    // %RETRY% 与 %DLQ% 是资源用途前缀，应保留；只删除它们之后匹配的 namespace% 部分。
    // 例子：%RETRY%flight_airpriceresourceapi_topic
    if (resource.startsWith("%RETRY%")) {
      prefix = "%RETRY%";
      // 处理后得到flight_airpriceresourceapi_topic
      resourceWithoutPrefix = resource.substring(prefix.length());
    } else if (resource.startsWith("%DLQ%")) {
      prefix = "%DLQ%";
      resourceWithoutPrefix = resource.substring(prefix.length());
    }

    String namespacePrefix = namespace + "%";
    // 例如 namespace=tenant，%RETRY%tenant%group 会变成 %RETRY%group；不匹配则保留原资源名。
    return resourceWithoutPrefix.startsWith(namespacePrefix)
        ? prefix + resourceWithoutPrefix.substring(namespacePrefix.length())
        : resource;
  }

  private RocketMqNamespaceUtil() {}

  private interface NamespaceAccessor {
    @Nullable
    String get(Object context);
  }
}
