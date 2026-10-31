package com.krachelfahd.kafkastreamsspringcloudstream.handlers;

import com.krachelfahd.kafkastreamsspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import java.util.Date;
import java.util.Random;
import java.util.function.Supplier;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import java.util.UUID;

import java.util.function.Consumer;

@Component
public class PageEventHandler {

    @Bean
    public Consumer<Message<PageEvent>> pageEventConsumer(){
        return (input)->{
            System.out.println("*******************");
            System.out.println(input.getPayload());
            System.out.println("*****************");
        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()->{
            return new PageEvent(
                    Math.random()>0.5?"P1":"P2",
                    Math.random()>0.5?"U1":"U2",
                    new Date(),
                    10+new Random().nextInt(10000)
            );
        };
    }
    @Bean
    public Supplier<Message<PageEvent>> fluxSupplier(){
        return ()->{
            PageEvent pageEvent =new PageEvent(
                    Math.random()>0.5?"P1":"P2",
                    Math.random()>0.5?"U1":"U2",
                    new Date(),
                    10+new Random().nextInt(10000));
            Message<PageEvent> message = MessageBuilder
                    .withPayload(pageEvent)
                    .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString()).build();
            return message;
        };
    }
}