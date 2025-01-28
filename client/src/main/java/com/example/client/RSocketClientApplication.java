package com.example.client;

import io.rsocket.core.Resume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.rsocket.RSocketRequester;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
public class RSocketClientApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RSocketClientApplication.class);
    private final RSocketRequester.Builder requesterBuilder;

    public RSocketClientApplication(RSocketRequester.Builder requesterBuilder) {
        this.requesterBuilder = requesterBuilder;
    }

    public static void main(String[] args) {
        SpringApplication.run(RSocketClientApplication.class, args);
    }

    @Override
    public void run(String... args) {
        RSocketRequester requester = null;
        try {
            // Configure the client with resume support
            requester = requesterBuilder
                    .rsocketConnector(connector -> connector
                            .resume(new Resume())
                    )
                    .connectTcp("localhost", 7878)
                    .block();  // Synchronously get the RSocketRequester

            // Send 10 messages to the server
            List<String> response = requester
                    .route("echo")
                    .data(
                            Flux.range(1, 1000)
                                    .map(i -> {
                                        log.info("Sending: " + i);
                                        return "Hello " + i;
                                    })
                                    .delayElements(Duration.ofSeconds(1)))
                    .retrieveFlux(String.class)
                    .collectList()
                    .block();

            System.out.println("Client received response: " + response);
        } finally {
            // Cleanly dispose the connection if it was established
            if (requester != null) {
                requester.rsocket().dispose();
                requester.rsocket().onClose().block();
            }
        }
    }

}
