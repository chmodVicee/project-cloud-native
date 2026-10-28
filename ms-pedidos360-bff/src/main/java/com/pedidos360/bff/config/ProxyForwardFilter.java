package com.pedidos360.bff.config;

import com.pedidos360.bff.security.JwtService;
import io.jsonwebtoken.Claims;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Set;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
@RequiredArgsConstructor
public class ProxyForwardFilter extends OncePerRequestFilter {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ProxyForwardFilter.class);

    private final RestClient restClient;
    private final JwtService jwtService;

    @Value("${services.users-url}")
    private String usersUrl;

    @Value("${services.catalog-url}")
    private String catalogUrl;

    @Value("${services.orders-url}")
    private String ordersUrl;

    @Value("${services.rabbit-admin-url}")
    private String rabbitAdminUrl;

    @Value("${public.base-url}")
    private String publicBaseUrl;

    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
            "te", "trailer", "transfer-encoding", "upgrade", "host", "content-length");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String path = request.getRequestURI();

        Target target = resolveTarget(path, request.getMethod());
        if (target == null) {
            filterChain.doFilter(request, response);
            return;
        }

        String userEmail = null;
        if (target.requiresAuth()) {
            Claims claims = authenticate(request, response);
            if (claims == null) {
                return;
            }
            userEmail = claims.getSubject();
            String role = claims.get("role", String.class);
            if (target.requiresAdmin() && !"ADMIN".equals(role)) {
                writeJson(response, HttpStatus.FORBIDDEN, "{\"error\":\"No tienes permisos para esta operación\"}");
                return;
            }
        }

        forward(request, response, target, userEmail);
    }

    private Claims authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            writeJson(response, HttpStatus.UNAUTHORIZED, "{\"error\":\"Token JWT requerido\"}");
            return null;
        }
        try {
            return jwtService.validate(header.substring(7));
        } catch (Exception ex) {
            log.warn("JWT inválido: {} - {}", ex.getClass().getSimpleName(), ex.getMessage());
            writeJson(response, HttpStatus.UNAUTHORIZED, "{\"error\":\"Token inválido o expirado\"}");
            return null;
        }
    }

    private void forward(HttpServletRequest request, HttpServletResponse response,
                         Target target, String userEmail) throws IOException {
        try {
            URI uri = buildUri(target.baseUrl(), request);
            HttpMethod method = HttpMethod.valueOf(request.getMethod());

            byte[] body = readBody(request);
            MediaType contentType = request.getContentType() != null
                    ? MediaType.parseMediaType(request.getContentType())
                    : null;

            var requestSpec = restClient.method(method)
                    .uri(uri)
                    .headers(headers -> {
                        copyRequestHeaders(request, headers);
                        addForwardedHeaders(request, headers);
                        if (userEmail != null && target.baseUrl().equals(ordersUrl)) {
                            headers.set("X-User-Email", userEmail);
                        }
                    });
            if (body.length > 0 && contentType != null) {
                requestSpec.contentType(contentType);
                requestSpec.body(body);
            }

            Forwarded forwarded = requestSpec.exchange((req, res) -> {
                byte[] bytes = res.getBody().readAllBytes();
                return new Forwarded(res.getStatusCode().value(), res.getHeaders(), bytes);
            });

            response.setStatus(forwarded.status());
            forwardHeaders(response, forwarded.headers(), forwarded.body().length);
            response.getOutputStream().write(forwarded.body());
        } catch (RestClientException ex) {
            writeJson(response, HttpStatus.BAD_GATEWAY, "{\"error\":\"Servicio interno no disponible\"}");
        }
    }

    private URI buildUri(String base, HttpServletRequest request) {
        String path = request.getRequestURI();
        String query = request.getQueryString();
        return URI.create(base + path + (query != null ? "?" + query : ""));
    }

    private void copyRequestHeaders(HttpServletRequest request, HttpHeaders headers) {
        Enumeration<String> names = request.getHeaderNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            if (HOP_BY_HOP.contains(name.toLowerCase())) {
                continue;
            }
            headers.remove(name);
            for (String value : Collections.list(request.getHeaders(name))) {
                headers.add(name, value);
            }
        }
    }

    private void addForwardedHeaders(HttpServletRequest request, HttpHeaders headers) {
        URI base = URI.create(publicBaseUrl);
        headers.set("X-Forwarded-Host", base.getHost() + (base.getPort() > 0 ? ":" + base.getPort() : ""));
        headers.set("X-Forwarded-Port", String.valueOf(base.getPort() > 0 ? base.getPort() : 80));
        headers.set("X-Forwarded-Proto", base.getScheme());
        headers.set("X-Forwarded-For", request.getRemoteAddr());
    }

    private void forwardHeaders(HttpServletResponse response, HttpHeaders headers, int bodyLength) {
        headers.forEach((name, values) -> {
            if (HOP_BY_HOP.contains(name.toLowerCase())) {
                return;
            }
            for (String value : values) {
                response.addHeader(name, value);
            }
        });
        response.setContentLength(bodyLength);
    }

    private byte[] readBody(HttpServletRequest request) throws IOException {
        String method = request.getMethod();
        if (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")) {
            return request.getInputStream().readAllBytes();
        }
        return new byte[0];
    }

    private void writeJson(HttpServletResponse response, HttpStatus status, String json) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getOutputStream().write(json.getBytes(StandardCharsets.UTF_8));
    }

    private Target resolveTarget(String path, String method) {
        if (isProxyPath(path)) {
            return new Target(usersUrl, false, false);
        }
        if (path.equals("/api/users")) {
            return new Target(usersUrl, true, true);
        }
        if (path.startsWith("/api/users")) {
            return new Target(usersUrl, true, false);
        }
        if (path.startsWith("/api/catalog")) {
            boolean write = List.of("POST", "PUT", "PATCH", "DELETE").contains(method);
            return new Target(catalogUrl, true, write);
        }
        if (path.startsWith("/api/orders")) {
            boolean admin = method.equals("PUT") && path.matches("^/api/orders/\\d+/status$");
            return new Target(ordersUrl, true, admin);
        }
        if (path.startsWith("/api/rabbit")) {
            return new Target(rabbitAdminUrl, true, true);
        }
        return null;
    }

    private boolean isProxyPath(String path) {
        return path.startsWith("/oauth2/")
                || path.startsWith("/login/oauth2/")
                || path.startsWith("/api/auth/");
    }

    private record Target(String baseUrl, boolean requiresAuth, boolean requiresAdmin) {
    }

    private record Forwarded(int status, HttpHeaders headers, byte[] body) {
    }
}