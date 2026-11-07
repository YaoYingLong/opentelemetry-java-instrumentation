/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** 统一请求头选择器的大小写，并为每个头名构造 rpc.request.header. 前缀的字符串数组属性键。 */
final class CapturedGrpcRequestHeadersUtil {
  private static final String RPC_REQUEST_HEADER_KEY_ATTRIBUTE_PREFIX = "rpc.request.header.";

  static List<String> lowercase(List<String> names) {
    return unmodifiableList(names.stream().map(s -> s.toLowerCase(Locale.ROOT)).collect(toList()));
  }

  static IncludeExclude lowercase(IncludeExclude selector) {
    return IncludeExclude.builder()
        .setIncluded(lowercase(selector.getIncluded()))
        .setExcluded(lowercase(selector.getExcluded()))
        .build();
  }

  static Map<String, AttributeKey<List<String>>> createLiteralRequestHeaderAttributeKeys(
      IncludeExclude selector) {
    return createLiteralAttributeKeys(selector, RPC_REQUEST_HEADER_KEY_ATTRIBUTE_PREFIX);
  }

  static AttributeKey<List<String>> requestHeaderAttributeKey(
      String metadataKey,
      Map<String, AttributeKey<List<String>>> literalRequestHeaderAttributeKeys) {
    return attributeKey(
        metadataKey, literalRequestHeaderAttributeKeys, RPC_REQUEST_HEADER_KEY_ATTRIBUTE_PREFIX);
  }

  private static Map<String, AttributeKey<List<String>>> createLiteralAttributeKeys(
      IncludeExclude selector, String prefix) {
    // 只预建配置中明确列出的头名；通配符和仅排除选择器的匹配结果来自请求，不保留为动态缓存。
    Map<String, AttributeKey<List<String>>> result = new HashMap<>();
    for (String pattern : selector.getIncluded()) {
      if (pattern.indexOf('*') == -1 && pattern.indexOf('?') == -1) {
        result.put(pattern, AttributeKey.stringArrayKey(prefix + pattern));
      }
    }
    return unmodifiableMap(result);
  }

  private static AttributeKey<List<String>> attributeKey(
      String metadataKey,
      Map<String, AttributeKey<List<String>>> literalRequestHeaderAttributeKeys,
      String prefix) {
    // 字面量头名复用预建键，其余按当前请求创建，不让远端可控的头名长期占用缓存。
    AttributeKey<List<String>> attributeKey = literalRequestHeaderAttributeKeys.get(metadataKey);
    return attributeKey != null ? attributeKey : AttributeKey.stringArrayKey(prefix + metadataKey);
  }

  private CapturedGrpcRequestHeadersUtil() {}
}
