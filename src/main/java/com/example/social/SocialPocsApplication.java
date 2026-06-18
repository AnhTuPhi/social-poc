package com.example.social;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SocialPocsApplication {
    public static void main(String[] args) {
        SpringApplication.run(SocialPocsApplication.class, args);
    }
}
