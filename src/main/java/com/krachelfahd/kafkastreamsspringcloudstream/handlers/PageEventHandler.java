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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import java.time.Duration;
import java.util.function.Function;

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
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction(){
        return (input)->
                input
                        .map((k,v)->new KeyValue<>(v.name(), v.duration()))
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                        .count(Materialized.as("count-store"))
                        .toStream()
                        .peek((k,v)->{
                            System.out.println("=======>  "+ k.key());
                            System.out.println("=======>  "+v);
                        })
                        .map((k,v)->new KeyValue<>(k.key(), v));
    }
}