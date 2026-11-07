/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6.internal;

import javax.annotation.Nullable;

/**
 * Holds the parsed server address and port from a gRPC target string.
 *
 * <p>解析结果只描述逻辑目标，不执行 DNS 查询。address 可能是主机、Unix socket 路径或完整自定义 target； port 为 null
 * 表示未提供或无法解析，不会在此推测默认端口。
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class ParsedTarget {

  private final String address;
  @Nullable private final Integer port;

  ParsedTarget(String address, @Nullable Integer port) {
    this.address = address;
    this.port = port;
  }

  public String getAddress() {
    return address;
  }

  @Nullable
  public Integer getPort() {
    return port;
  }
}
