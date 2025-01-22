package com.example.server;

import io.rsocket.core.RSocketServer;
import io.rsocket.core.Resume;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.rsocket.server.RSocketServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class RSocketServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RSocketServerApplication.class, args);
    }

    // 1) Enable resume programmatically
    @Bean
    public RSocketServerCustomizer rSocketServerCustomizer() {
        return (RSocketServer rSocketServer) -> {
            rSocketServer.resume(new Resume());
        };
    }

    // 2) Example controller that handles "echo" route
    @Controller
    static class RSocketController {

        @MessageMapping("echo")
        Flux<String> echo(Flux<String> message) {
            return message.map(s -> "Echo: " + s);
        }
    }
}
