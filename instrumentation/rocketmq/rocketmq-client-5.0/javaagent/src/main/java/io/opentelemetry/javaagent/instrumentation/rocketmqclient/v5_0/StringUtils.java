/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import javax.annotation.Nullable;

/** 将 SDK/协议用空字符串表示的“未设置”转换为 null，供属性提取器省略该字段；不裁剪非空内容。 */
final class StringUtils {

  @Nullable
  static String emptyToNull(@Nullable String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private StringUtils() {}
}
