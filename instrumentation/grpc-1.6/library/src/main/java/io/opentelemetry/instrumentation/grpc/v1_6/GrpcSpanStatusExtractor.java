/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import static java.util.Arrays.asList;

import com.google.errorprone.annotations.Immutable;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusExtractor;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * 将 gRPC 结果映射到 span 状态：客户端把所有非 OK 状态视为错误，服务端只把下列服务端错误状态标为 ERROR。 因而同一个 CANCELLED/INVALID_ARGUMENT
 * 等结果在两端的 span 状态可以不同。
 */
enum GrpcSpanStatusExtractor implements SpanStatusExtractor<GrpcRequest, Status> {
  CLIENT(GrpcSpanStatusExtractor::isClientError),
  SERVER(GrpcSpanStatusExtractor::isServerError);

  private static final Set<Status.Code> serverErrorStatuses = new HashSet<>();

  static {
    serverErrorStatuses.addAll(
        asList(
            Status.Code.UNKNOWN,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.UNIMPLEMENTED,
            Status.Code.INTERNAL,
            Status.Code.UNAVAILABLE,
            Status.Code.DATA_LOSS));
  }

  private final ErrorStatusPredicate isError;

  GrpcSpanStatusExtractor(ErrorStatusPredicate isError) {
    this.isError = isError;
  }

  @Override
  public void extract(
      SpanStatusBuilder spanStatusBuilder,
      GrpcRequest request,
      @Nullable Status status,
      @Nullable Throwable error) {
    // 优先使用调用结束时提供的协议状态；缺失时才从 gRPC 状态异常中取出，否则使用默认异常判定。
    if (status == null) {
      if (error instanceof StatusRuntimeException) {
        status = ((StatusRuntimeException) error).getStatus();
      } else if (error instanceof StatusException) {
        status = ((StatusException) error).getStatus();
      }
    }
    if (status != null) {
      // 非错误状态保持默认 UNSET，不主动标记 OK。
      if (isError.test(status)) {
        spanStatusBuilder.setStatus(StatusCode.ERROR);
      }
    } else {
      SpanStatusExtractor.getDefault().extract(spanStatusBuilder, request, status, error);
    }
  }

  static boolean isServerError(Status status) {
    return serverErrorStatuses.contains(status.getCode());
  }

  private static boolean isClientError(Status status) {
    return !status.isOk();
  }

  @Immutable
  private interface ErrorStatusPredicate extends Predicate<Status> {}
}
