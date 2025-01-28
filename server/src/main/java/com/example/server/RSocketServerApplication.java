package com.example.server;

import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SpringBootApplication
public class RSocketServerApplication {
    private static final Logger log = LoggerFactory.getLogger(RSocketServerApplication.class);

    public static void main(String[] args) {
        var redis = configureRedis();
        var springApplication = new SpringApplication(RSocketServerApplication.class);
        springApplication.addListeners((ApplicationStartingEvent e) -> redis.start());
        springApplication.addListeners((ContextClosedEvent e) -> {
            if (!TestcontainersConfiguration.getInstance().environmentSupportsReuse()) {
                redis.stop();
            }
        });
        springApplication.run(
                "--spring.data.redis.host=localhost"
        );
    }

    @Bean
    @ConditionalOnProperty("spring.data.redis.host")
    public RedisTemplate<String, byte[]> redisByteArrayTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, byte[]> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        // Use String serialization for keys
        template.setKeySerializer(RedisSerializer.string());

        // Use byte-array (binary) serialization for values
        template.setValueSerializer(RedisSerializer.byteArray());

        // Also use byte-array for List operations
        template.setDefaultSerializer(RedisSerializer.byteArray());

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RSocketServerCustomizer rSocketServerCustomizer(RedisTemplate<String, byte[]> redisTemplate) {
        return (RSocketServer rSocketServer) -> {
            rSocketServer.resume(new Resume()
                    .sessionDuration(Duration.ofMinutes(5))
                    .cleanupStoreOnKeepAlive()
                    .storeFactory(t -> new RedisResumableFramesStore("server", t, redisTemplate))
            );
        };
    }

    // 2) Example controller that handles "echo" route
    @Controller
    static class RSocketController {

        @MessageMapping("echo")
        Flux<String> echo(Flux<String> message) {
            return message.map(s -> {
                log.info("Received message: {}", s);
                return "Echo: " + s;
            });
        }
    }

    @SuppressWarnings({"rawtypes"})
    private static GenericContainer configureRedis() {
        return new GenericContainer("redis:6.0.9") {
            @Override
            protected void configure() {
                super.addFixedExposedPort(6379, 6379);
            }
        };
    }
}
