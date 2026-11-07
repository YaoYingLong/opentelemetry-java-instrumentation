/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * Parses gRPC target strings into server address and port per the <a
 * href="https://grpc.github.io/grpc/core/md_doc_naming.html">gRPC Name Resolution spec</a> and <a
 * href="https://github.com/open-telemetry/semantic-conventions/pull/3317">semantic conventions</a>.
 *
 * <p>把 target 转成遥测中的逻辑服务地址，不执行名称解析或建立连接。DNS 目标取 endpoint，Unix 目标取路径， 其他 resolver
 * 目标通常保留完整字符串；无法提供目标地址时返回 null，由 GrpcRequest 再尝试 authority。
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class GrpcTargetParser {

  @Nullable private static final Method getDefaultRegistryMethod;
  @Nullable private static final Method getDefaultSchemeMethod;
  @Nullable private static final Method getProviderForSchemeMethod;

  static {
    Method defaultRegistryMethod = null;
    Method defaultSchemeMethod = null;
    Method providerForSchemeMethod = null;
    try {
      Class<?> registryClass = Class.forName("io.grpc.NameResolverRegistry");
      defaultRegistryMethod = registryClass.getMethod("getDefaultRegistry");
      defaultSchemeMethod = registryClass.getMethod("getDefaultScheme");
      providerForSchemeMethod = registryClass.getMethod("getProviderForScheme", String.class);
    } catch (ClassNotFoundException | NoSuchMethodException ignored) {
      // NameResolverRegistry is available since gRPC 1.21
    }
    getDefaultRegistryMethod = defaultRegistryMethod;
    getDefaultSchemeMethod = defaultSchemeMethod;
    getProviderForSchemeMethod = providerForSchemeMethod;
  }

  @Nullable
  public static ParsedTarget parse(@Nullable String target) {
    // 用户原始 target 可能像 host:port，也可能像 resolver:值，需要结合当前 resolver 注册情况消除歧义。
    return parseTarget(target, false);
  }

  @Nullable
  public static ParsedTarget parseComputedTarget(@Nullable String target) {
    // gRPC 已经选择 resolver 的 target，不再用当前默认 resolver 重新解释其单冒号形式。
    return parseTarget(target, true);
  }

  @Nullable
  public static String createDnsTargetFromUri(@Nullable URI uri) {
    // 从 URI 的 authority 构造 DNS target，移除 user-info，并转义 IPv6 方括号使其能出现在 URI 路径中。
    if (uri == null) {
      return null;
    }
    String authority = uri.getRawAuthority();
    if (authority == null) {
      return null;
    }
    int userInfoEnd = authority.lastIndexOf('@');
    if (userInfoEnd >= 0) {
      authority = authority.substring(userInfoEnd + 1);
    }
    authority = authority.replace("[", "%5B").replace("]", "%5D");
    return "dns:///" + authority;
  }

  @Nullable
  private static ParsedTarget parseTarget(@Nullable String target, boolean computedTarget) {
    if (target == null || target.isEmpty()) {
      return null;
    }

    int schemeEnd = target.indexOf("://");
    // 显式 scheme:// 可以直接按 scheme 分流；没有它时必须先区分 IPv6、裸主机和单冒号写法。
    if (schemeEnd == -1) {
      // Bracketed IPv6 like "[::1]" or "[::1]:8080"
      if (target.startsWith("[")) {
        return parseHostPort(target);
      }
      // Check for single-colon scheme like "dns:endpoint" or "unix:/path"
      int colonIndex = target.indexOf(':');
      if (colonIndex == -1) {
        // No scheme, no port — just a host name
        return new ParsedTarget(target, null);
      }

      String originalScheme = target.substring(0, colonIndex);
      String scheme = originalScheme.toLowerCase(Locale.ROOT);
      if (isKnownScheme(scheme)) {
        return parseSingleColonScheme(scheme, target);
      }

      if (isValidScheme(originalScheme)) {
        // 例如 custom:123 的前缀既可能是主机也可能是 resolver 名称。
        // 已计算目标、已有对应 provider 或默认 resolver 非 DNS 时，保留整个 target，不擅自拆出端口。
        if (computedTarget) {
          return new ParsedTarget(target, null);
        }
        ResolverSelection resolverSelection = getResolverSelection(scheme);
        if (resolverSelection.hasProvider) {
          return new ParsedTarget(target, null);
        }
        if (!"dns".equals(resolverSelection.defaultScheme)) {
          return new ParsedTarget(target, null);
        }
      }

      return parseHostPort(target);
    }

    String scheme = target.substring(0, schemeEnd).toLowerCase(Locale.ROOT);

    if ("dns".equals(scheme)) {
      return parseDnsScheme(target);
    }

    if ("unix".equals(scheme) || "unix-abstract".equals(scheme)) {
      return parseUnixScheme(target);
    }

    if ("directaddress".equals(scheme)) {
      // 此内部目标本身不能提供可用 endpoint，返回 null 让调用方回退到 channel authority。
      return null;
    }

    // Unknown scheme with "://" — use full target string as address, no port
    return new ParsedTarget(target, null);
  }

  /**
   * Parses an HTTP/2 authority of the form {@code host}, {@code host:port}, or {@code [ipv6]:port}
   * into address and port. Returns {@code null} for {@code null}/empty input.
   */
  @Nullable
  public static ParsedTarget parseAuthority(@Nullable String authority) {
    // authority 已明确是主机/端口语义，不参与 resolver scheme 判断。
    if (authority == null || authority.isEmpty()) {
      return null;
    }
    return parseHostPort(authority);
  }

  @Nullable
  private static ParsedTarget parseSingleColonScheme(String scheme, String target) {
    if ("dns".equals(scheme)) {
      return parseDnsScheme(target);
    }

    if ("unix".equals(scheme) || "unix-abstract".equals(scheme)) {
      return parseUnixScheme(target);
    }

    // ipv4:, ipv6:, or other — full target as address
    return new ParsedTarget(target, null);
  }

  @Nullable
  private static ParsedTarget parseDnsScheme(String target) {
    // dns://resolver/host:port 中，resolver 是解析服务的 authority，真正的 RPC 目标在 path 中。
    // dns:host:port 是 opaque URI，因此从 schemeSpecificPart 读取。
    try {
      URI uri = new URI(target);
      String endpoint = uri.isOpaque() ? uri.getSchemeSpecificPart() : uri.getPath();
      if (endpoint == null) {
        return null;
      }
      if (endpoint.startsWith("/")) {
        endpoint = endpoint.substring(1);
      }
      return parseHostPort(endpoint);
    } catch (URISyntaxException ignored) {
      String rest = target.substring("dns:".length());
      if (rest.isEmpty() || "//".equals(rest)) {
        return null;
      }
      return new ParsedTarget(target, null);
    }
  }

  @Nullable
  private static ParsedTarget parseUnixScheme(String target) {
    // Unix socket 路径本身就是地址，没有网络端口；支持 unix 和 unix-abstract 两种 scheme。
    try {
      URI uri = new URI(target);
      String endpoint = uri.isOpaque() ? uri.getSchemeSpecificPart() : uri.getPath();
      return endpoint == null || endpoint.isEmpty() ? null : new ParsedTarget(endpoint, null);
    } catch (URISyntaxException ignored) {
      String rest = target.substring(target.indexOf(':') + 1);
      if (rest.startsWith("//")) {
        rest = rest.substring(2);
      }
      // unix://authority/path — the path (after authority) is the address
      int slashIndex = rest.indexOf('/');
      String endpoint = slashIndex != -1 ? rest.substring(slashIndex) : rest;
      return endpoint.isEmpty() ? null : new ParsedTarget(endpoint, null);
    }
  }

  @Nullable
  private static ParsedTarget parseHostPort(String hostPort) {
    // 带方括号的 IPv6 可明确区分端口；裸 IPv6 含多个冒号时整体保留，避免把末段地址误认成端口。
    if (hostPort.isEmpty()) {
      return null;
    }

    // Handle IPv6 in brackets: [::1]:8080
    if (hostPort.startsWith("[")) {
      int closeBracket = hostPort.indexOf(']');
      if (closeBracket != -1) {
        String host = hostPort.substring(1, closeBracket);
        if (host.isEmpty()) {
          return null;
        }
        if (closeBracket + 1 < hostPort.length() && hostPort.charAt(closeBracket + 1) == ':') {
          Integer port = parsePort(hostPort.substring(closeBracket + 2));
          return new ParsedTarget(host, port);
        }
        return new ParsedTarget(host, null);
      }
    }

    int lastColon = hostPort.lastIndexOf(':');
    if (lastColon == -1) {
      return new ParsedTarget(hostPort, null);
    }

    // Multiple colons — likely bare IPv6, use as-is
    int firstColon = hostPort.indexOf(':');
    if (firstColon != lastColon) {
      return new ParsedTarget(hostPort, null);
    }

    String host = hostPort.substring(0, lastColon);
    if (host.isEmpty()) {
      return null;
    }
    Integer port = parsePort(hostPort.substring(lastColon + 1));
    return new ParsedTarget(host, port);
  }

  @Nullable
  private static Integer parsePort(String portStr) {
    // 无效或越界端口仅省略 port，调用方仍可保留已经解析出的主机部分。
    try {
      int port = Integer.parseInt(portStr);
      if (port >= 0 && port <= 65535) {
        return port;
      }
    } catch (NumberFormatException ignored) {
      // ignore
    }
    return null;
  }

  private static boolean isKnownScheme(String scheme) {
    return "dns".equals(scheme)
        || "unix".equals(scheme)
        || "unix-abstract".equals(scheme)
        || "ipv4".equals(scheme)
        || "ipv6".equals(scheme)
        || "xds".equals(scheme);
  }

  private static boolean isValidScheme(String scheme) {
    if (scheme.isEmpty() || !isAsciiLetter(scheme.charAt(0))) {
      return false;
    }
    for (int i = 1; i < scheme.length(); i++) {
      char c = scheme.charAt(i);
      if (!isAsciiLetter(c) && (c < '0' || c > '9') && c != '+' && c != '-' && c != '.') {
        return false;
      }
    }
    return true;
  }

  private static boolean isAsciiLetter(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  }

  private static ResolverSelection getResolverSelection(String scheme) {
    // 这里只查询 provider 是否注册及默认 scheme，不调用 resolver 做网络查询。
    // 较旧版本缺少注册表 API 或反射失败时，按无自定义 provider、默认 DNS 的分支处理。
    if (getDefaultRegistryMethod == null
        || getDefaultSchemeMethod == null
        || getProviderForSchemeMethod == null) {
      return new ResolverSelection(false, "dns");
    }
    try {
      Object registry = getDefaultRegistryMethod.invoke(null);
      boolean hasProvider = getProviderForSchemeMethod.invoke(registry, scheme) != null;
      String defaultScheme = (String) getDefaultSchemeMethod.invoke(registry);
      return new ResolverSelection(hasProvider, defaultScheme.toLowerCase(Locale.ROOT));
    } catch (IllegalAccessException | InvocationTargetException ignored) {
      return new ResolverSelection(false, "dns");
    }
  }

  private GrpcTargetParser() {}

  private static final class ResolverSelection {
    private final boolean hasProvider;
    private final String defaultScheme;

    private ResolverSelection(boolean hasProvider, String defaultScheme) {
      this.hasProvider = hasProvider;
      this.defaultScheme = defaultScheme;
    }
  }
}
