/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.Metadata;
import io.opentelemetry.context.propagation.TextMapSetter;
import javax.annotation.Nullable;

/** 将 propagator 生成的文本传播字段写入 gRPC 请求头。 */
enum MetadataSetter implements TextMapSetter<Metadata> {
  INSTANCE;

  @Override
  public void set(@Nullable Metadata carrier, String key, String value) {
    if (carrier == null) {
      return;
    }
    Metadata.Key<String> metadataKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
    // Metadata 支持同名多值。注入前删除旧值，避免一个传播字段同时携带旧链路和当前 RPC 的值。
    carrier.removeAll(metadataKey);
    carrier.put(metadataKey, value);
  }
}
