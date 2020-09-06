package dev.binhcn;

import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.sampler.Sampler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.kafka11.KafkaSender;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.lang.System.out;

public class HelloConsumer {
    public static void main(String[] args) {

        final Config config = ConfigFactory.load();
        /* START TRACING INSTRUMENTATION */
        final KafkaSender kafkaSender = KafkaSender.create("localhost:9092");

        final AsyncReporter reporter = AsyncReporter.builder(kafkaSender).build();
        final Tracing tracing = Tracing.newBuilder().localServiceName("hello-consumer")
                .sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).build();
        final KafkaTracing kafkaTracing = KafkaTracing.newBuilder(tracing)
                .remoteServiceName("kafka").build();
        /* END TRACING INSTRUMENTATION */

        final Properties consumerConfigs = new Properties();
        consumerConfigs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString("kafka.bootstrap-servers"));
        consumerConfigs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerConfigs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerConfigs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello-consumer");
        consumerConfigs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                OffsetResetStrategy.EARLIEST.name().toLowerCase());

        final KafkaConsumer kafkaConsumer = new KafkaConsumer<String, String>(consumerConfigs);
        final Consumer tracingConsumer = kafkaTracing.consumer(kafkaConsumer);

        tracingConsumer.subscribe(Collections.singletonList("UMUserInfoChange"));

        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> records = tracingConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            for (ConsumerRecord record : records) {
                brave.Span span = kafkaTracing.nextSpan(record).name("print-hello")
                        .start();
                span.annotate("starting printing");
                out.println(String.format("Record: %s", record));
                span.annotate("printing finished");
                span.finish();
            }
        }
    }
}
