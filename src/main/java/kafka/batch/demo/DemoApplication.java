package kafka.batch.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@Slf4j
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    //@Bean
    public ApplicationRunner runner(KafkaTemplate<String, GenericRecord> template) {
        return args -> fillAndSend(template);
    }

    @KafkaListener(id = "b548557866hh", topics = "TestTopic")
    public void listen(List<ConsumerRecord<String, GenericRecord>> records) {
        log.info("start of batch receive. Size::{}", records.size());
        for (ConsumerRecord<String, GenericRecord> record : records) {
            log.info("{}", record);
        }
        log.info("Complete kafka bach processing");
    }

    private void fillAndSend(KafkaTemplate<String, GenericRecord> template) {
        Schema schema = null;
        long baseLong = 79180000000L;
        for (int i = 0; i < 150_000; i++) {
            Map<String, Object> inputData = new HashMap<>();
            inputData.put("MSISDN", baseLong + i);
            if (schema == null) {
                schema = getAvro(inputData);
            }
            GenericRecord avroRecord = new GenericData.Record(schema);
            inputData.forEach((k, v) -> {
                if (v instanceof ZonedDateTime) {
                    long millis = ChronoUnit.MILLIS.between(Instant.EPOCH, (ZonedDateTime) v);
                    avroRecord.put(k, millis);
                } else if (v instanceof BigDecimal) {
                    avroRecord.put(k, ByteBuffer.wrap(v.toString().getBytes()));
                } else {
                    avroRecord.put(k, v);
                }
            });
            template.send("TestTopic", inputData.get("MSISDN").toString(), avroRecord);
        }

    }

    private Schema getAvro(Map<String, Object> inputData) {
        SchemaBuilder.FieldAssembler<Schema> schemaFieldAssembler = SchemaBuilder.record("AvroEventRequest2")
                .namespace("ru.mts.fms.avro.test")
                .fields();

        for (Map.Entry<String, Object> entry : inputData.entrySet()) {
            if (entry.getValue() instanceof String) {
                schemaFieldAssembler = schemaFieldAssembler.optionalString(entry.getKey());
            } else if (entry.getValue() instanceof Integer) {
                schemaFieldAssembler = schemaFieldAssembler.optionalInt(entry.getKey());
            } else if (entry.getValue() instanceof Long) {
                schemaFieldAssembler = schemaFieldAssembler.optionalLong(entry.getKey());
            } else if (entry.getValue() instanceof BigDecimal) {
                Schema decimalSchema = LogicalTypes.decimal(9)
                        .addToSchema(Schema.create(Schema.Type.FIXED));
                schemaFieldAssembler = schemaFieldAssembler.name(entry.getKey())
                        .type(decimalSchema).noDefault();
            }
        }

        return schemaFieldAssembler.endRecord();
    }
}
