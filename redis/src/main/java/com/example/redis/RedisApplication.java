package com.example.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartingEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

@SpringBootApplication
public class RedisApplication {
    private static final Logger log = LoggerFactory.getLogger(com.example.redis.RedisApplication.class);

    public static void main(String[] args) {
        var redis = configureRedis();
        var springApplication = new SpringApplication(com.example.redis.RedisApplication.class);
        springApplication.addListeners((ApplicationStartingEvent e) -> redis.start());
        springApplication.addListeners((ContextClosedEvent e) -> {
            if (!TestcontainersConfiguration.getInstance().environmentSupportsReuse()) {
                redis.stop();
            }
        });
        springApplication.run(
                "--spring.data.redis.host=localhost"
        );
        // keep it running
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
