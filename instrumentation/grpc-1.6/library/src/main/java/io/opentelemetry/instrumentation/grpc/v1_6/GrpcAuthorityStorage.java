/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.ServerCall;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import javax.annotation.Nullable;

/**
 * In case a {@link ServerCall} implementation does not implement {@link ServerCall#getAuthority()}
 * like armeria, this utility class should be used to provide the authority instead
 *
 * <p>适配框架在创建调用时写入 authority，TracingServerInterceptor 在原生 getAuthority 返回 null 时读取。 数据按 ServerCall
 * 实例关联，只补充逻辑服务地址的来源，不替代连接上的远端 socket 地址。
 */
public final class GrpcAuthorityStorage {

  private static final VirtualField<ServerCall<?, ?>, String> AUTHORITY_FIELD =
      VirtualField.find(ServerCall.class, String.class);

  private GrpcAuthorityStorage() {}

  public static void setAuthority(ServerCall<?, ?> call, String authority) {
    AUTHORITY_FIELD.set(call, authority);
  }

  @Nullable
  static String getAuthority(ServerCall<?, ?> call) {
    return AUTHORITY_FIELD.get(call);
  }
}
