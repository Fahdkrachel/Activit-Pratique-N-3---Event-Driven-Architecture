package com.krachelfahd.kafkastreamsspringcloudstream.handlers;

import com.krachelfahd.kafkastreamsspringcloudstream.events.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

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
}