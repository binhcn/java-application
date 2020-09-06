package dev.longhb.lab.zipkin;

import brave.ScopedSpan;
import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import brave.sampler.Sampler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.kafka11.KafkaSender;

import java.io.IOException;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class HelloClient {

    public static void main(String[] args) throws InterruptedException, IOException {

        final Config config = ConfigFactory.load();

        /* START TRACING INSTRUMENTATION */
        final KafkaSender kafkaSender = KafkaSender.create(config.getString("kafka.bootstrap-servers"));

        final AsyncReporter reporter = AsyncReporter.builder(kafkaSender).build();
        final Tracing tracing = Tracing.newBuilder().localServiceName("hello-client")
                .sampler(Sampler.ALWAYS_SAMPLE).spanReporter(reporter).build();

        final KafkaTracing kafkaTracing = KafkaTracing.newBuilder(tracing)
                .remoteServiceName("kafka").build();

        final Tracer tracer = Tracing.currentTracer();
        /* END TRACING INSTRUMENTATION */

        final Properties producerConfigs = new Properties();
        producerConfigs.setProperty(BOOTSTRAP_SERVERS_CONFIG,
                config.getString("kafka.bootstrap-servers"));
        producerConfigs.setProperty(KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerConfigs.setProperty(VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        final KafkaProducer kafkaProducer = new KafkaProducer<String, String>(producerConfigs);
        final Producer tracedKafkaProducer = kafkaTracing.producer(kafkaProducer);

        /* START OPERATION */
        ScopedSpan span = tracer.startScopedSpan("call-hello");
        span.tag("name", "hi Long");
        span.annotate("starting operation");

        span.annotate("sending message to kafka");
        tracedKafkaProducer.send(new ProducerRecord<>("UMUserInfoChange", "hi Long"));
        span.annotate("complete operation");
        span.finish();

        /* END OPERATION */

        Thread.sleep(10_000);
    }

}
