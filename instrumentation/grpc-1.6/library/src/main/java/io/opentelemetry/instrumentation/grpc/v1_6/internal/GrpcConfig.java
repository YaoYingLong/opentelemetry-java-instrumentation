/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6.internal;

import static java.util.Collections.emptyList;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.incubator.config.DeclarativeConfigProperties;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import java.util.List;
import javax.annotation.Nullable;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 *
 * <p>读取客户端/服务端各自的请求头选择器；使用统一配置视图，使后续采集逻辑只依赖 IncludeExclude。
 */
public class GrpcConfig {

  @Nullable private final IncludeExclude clientRequestHeaders;
  @Nullable private final IncludeExclude serverRequestHeaders;

  public static GrpcConfig create(OpenTelemetry openTelemetry) {
    return new GrpcConfig(DeclarativeConfigUtil.getInstrumentationConfig(openTelemetry, "grpc"));
  }

  GrpcConfig(DeclarativeConfigProperties config) {
    clientRequestHeaders = getRequestHeaders(config, "client");
    serverRequestHeaders = getRequestHeaders(config, "server");
  }

  @Nullable
  public IncludeExclude getClientRequestHeaders() {
    return clientRequestHeaders;
  }

  @Nullable
  public IncludeExclude getServerRequestHeaders() {
    return serverRequestHeaders;
  }

  @Nullable
  private static IncludeExclude getRequestHeaders(DeclarativeConfigProperties config, String side) {
    // 两侧独立配置 included/excluded；缺省列表转为空列表，排除优先等匹配规则由 IncludeExclude 实现。
    DeclarativeConfigProperties requestHeaders = config.get(side).get("request_headers");
    List<String> included = requestHeaders.getScalarList("included", String.class);
    List<String> excluded = requestHeaders.getScalarList("excluded", String.class);
    IncludeExclude selector =
        IncludeExclude.builder()
            .setIncluded(included == null ? emptyList() : included)
            .setExcluded(excluded == null ? emptyList() : excluded)
            .build();
    // an empty selector is equivalent to no selector at all, matching flat configuration where
    // empty property values cannot be distinguished from unset ones
    if (!selector.isEmpty()) {
      // 仅配置 excluded 仍是有效选择器，含义是采集除排除项之外的所有合法 ASCII 头。
      return selector;
    }
    return null;
  }
}
