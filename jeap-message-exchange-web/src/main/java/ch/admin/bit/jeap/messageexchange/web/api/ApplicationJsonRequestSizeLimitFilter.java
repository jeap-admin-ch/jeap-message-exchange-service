package ch.admin.bit.jeap.messageexchange.web.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.security.web.firewall.RequestRejectedException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
@Slf4j
@RequiredArgsConstructor
public class ApplicationJsonRequestSizeLimitFilter extends OncePerRequestFilter {

    private final MessageExchangeApiProperties properties;

    @Override
    protected void doFilterInternal(@NotNull HttpServletRequest request, @NotNull HttpServletResponse response, @NotNull FilterChain filterChain)
            throws ServletException, IOException {
        if (request.getContentLengthLong() > properties.getMaxRequestBodySizeInBytes()) {
            log.warn("Content length {} exceeds the limit of {}", request.getContentLengthLong(), properties.getMaxRequestBodySizeInBytes());
            throw new RequestRejectedException("Request content exceeded limit of " + properties.getMaxRequestBodySizeInBytes() + " bytes");
        }
        filterChain.doFilter(request, response);
    }
}