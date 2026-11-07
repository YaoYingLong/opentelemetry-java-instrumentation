/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import static io.opentelemetry.instrumentation.grpc.v1_6.CapturedGrpcRequestHeadersUtil.createLiteralRequestHeaderAttributeKeys;
import static io.opentelemetry.instrumentation.grpc.v1_6.CapturedGrpcRequestHeadersUtil.lowercase;
import static io.opentelemetry.instrumentation.grpc.v1_6.CapturedGrpcRequestHeadersUtil.requestHeaderAttributeKey;
import static java.util.Collections.emptyMap;

import io.grpc.Metadata;
import io.grpc.Status;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;

/** 补充 gRPC 状态名称及按配置选择的 ASCII 请求头，输出到 RPC 的属性集合。 */
final class GrpcAttributesExtractor implements AttributesExtractor<GrpcRequest, Status> {

  // copied from RpcIncubatingAttributes
  private static final AttributeKey<String> RPC_STATUS_CODE =
      AttributeKey.stringKey("rpc.status_code");
  private final GrpcRpcAttributesGetter getter;
  @Nullable private final IncludeExclude requestHeaders;

  // Only literal included names are stored here. We intentionally do not cache wildcard or
  // exclude-only matches, even in a bounded cache: peer-controlled names could churn its entries,
  // making allocation behavior depend on prior traffic. Creating those attribute keys on demand
  // provides a fixed per-capture cost with no peer-controlled retained state, while literal
  // configured keys remain allocation-free.
  private final Map<String, AttributeKey<List<String>>> literalRequestHeaderAttributeKeys;

  GrpcAttributesExtractor(GrpcRpcAttributesGetter getter, @Nullable IncludeExclude requestHeaders) {
    // 未配置/空选择器均表示不采集请求头；非空选择器统一小写以实现不区分大小写的匹配。
    this.getter = getter;
    if (requestHeaders == null || requestHeaders.isEmpty()) {
      this.requestHeaders = null;
      literalRequestHeaderAttributeKeys = emptyMap();
    } else {
      this.requestHeaders = lowercase(requestHeaders);
      literalRequestHeaderAttributeKeys =
          createLiteralRequestHeaderAttributeKeys(this.requestHeaders);
    }
  }

  @Override
  public void onStart(AttributesBuilder attributes, Context parentContext, GrpcRequest request) {
    // 统一推迟到 onEnd：客户端启动 span 时还没有 Metadata，要等 ClientCall.start 才补入。
  }

  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      GrpcRequest request,
      @Nullable Status status,
      @Nullable Throwable error) {
    if (status != null) {
      attributes.put(RPC_STATUS_CODE, status.getCode().name());
    }
    if (requestHeaders == null || request.getMetadata() == null) {
      return;
    }
    for (String key : request.getMetadata().keys()) {
      // 遍历实际存在的头，支持通配符和仅排除配置；排除模式优先级由 IncludeExclude.matches 统一处理。
      // binary metadata and HTTP/2 pseudo-headers are never captured, even when the selector
      // matches, because reading them below with Metadata.Key.of() would throw
      if (!isAsciiMetadataKey(key)) {
        continue;
      }
      // gRPC lowercases metadata key names, but normalize defensively so that matching is
      // case-insensitive as documented and an excluded key cannot be captured because a transport
      // surfaced its name with different casing
      String metadataKey = key.toLowerCase(Locale.ROOT);
      if (!requestHeaders.matches(metadataKey)) {
        continue;
      }
      List<String> value = getter.metadataValue(request, metadataKey);
      // 同名头可能有多个值，作为字符串数组保存，不能只取最后一个值。
      if (!value.isEmpty()) {
        attributes.put(
            requestHeaderAttributeKey(metadataKey, literalRequestHeaderAttributeKeys), value);
      }
    }
  }

  // Returns whether the key names ASCII metadata that Metadata.Key.of() accepts. Metadata received
  // from a peer can contain HTTP/2 pseudo-headers and header names built from HTTP token characters
  // that gRPC does not allow in a metadata key.
  private static boolean isAsciiMetadataKey(String key) {
    // 这是读取远端 Metadata 的边界：-bin 使用二进制 marshaller，伪头及非法字符也不能交给 ASCII Key.of。
    if (key.isEmpty() || key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return false;
    }
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      // the character set accepted by Metadata.Key, which lowercases the name before validating it
      boolean valid =
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '-'
              || c == '_'
              || c == '.';
      if (!valid) {
        return false;
      }
    }
    return true;
  }
}
