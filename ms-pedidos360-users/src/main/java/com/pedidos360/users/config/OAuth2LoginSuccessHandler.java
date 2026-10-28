package com.pedidos360.users.config;

import com.pedidos360.users.model.Role;
import com.pedidos360.users.model.User;
import com.pedidos360.users.repository.UserRepository;
import com.pedidos360.users.security.JwtService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class OAuth2LoginSuccessHandler implements AuthenticationSuccessHandler {

    private final UserRepository userRepository;
    private final JwtService jwtService;

    @Value("${frontend.base-url}")
    private String spaBaseUrl;

    @Value("${app.security.azure-admin-domain:duoc.cl}")
    private String azureAdminDomain;

    @Override
    public void onAuthenticationSuccess(
            HttpServletRequest request,
            HttpServletResponse response,
            Authentication authentication
    ) throws IOException {
        String email = extractEmail(authentication);
        Role role = resolveRole(authentication, email);

        User user = userRepository.findByEmail(email)
                .orElseGet(() -> userRepository.save(User.builder()
                        .username(email)
                        .email(email)
                        .password(UUID.randomUUID().toString())
                        .role(Role.USER)
                        .build()));

        if (role == Role.ADMIN && user.getRole() != Role.ADMIN) {
            user.setRole(Role.ADMIN);
            userRepository.save(user);
        }

        String token = jwtService.generateToken(user);

        response.setStatus(HttpServletResponse.SC_FOUND);
        String baseUrl = spaBaseUrl.endsWith("/")
                ? spaBaseUrl.substring(0, spaBaseUrl.length() - 1)
                : spaBaseUrl;
        response.sendRedirect(baseUrl + "/auth/callback?token=" + token);
    }

    private Role resolveRole(Authentication authentication, String email) {
        if (authentication instanceof OAuth2AuthenticationToken token
                && "azure".equals(token.getAuthorizedClientRegistrationId())
                && email != null
                && email.toLowerCase().endsWith("@" + azureAdminDomain.toLowerCase())) {
            return Role.ADMIN;
        }
        return Role.USER;
    }

    private String extractEmail(Authentication authentication) {
        Object principal = authentication.getPrincipal();
        if (principal instanceof OidcUser oidcUser) {
            if (oidcUser.getEmail() != null) return oidcUser.getEmail();
            Object preferredUsername = oidcUser.getAttribute("preferred_username");
            if (preferredUsername != null) return String.valueOf(preferredUsername);
        } else if (principal instanceof OAuth2User oAuth2User) {
            Object email = oAuth2User.getAttribute("email");
            if (email != null) return String.valueOf(email);
        }
        throw new IllegalArgumentException("No se pudo obtener el email del usuario OAuth2");
    }
}