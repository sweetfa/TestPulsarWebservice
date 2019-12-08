package au.com.bushlife.integration.test.utils.pulsar;

import static java.text.MessageFormat.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController("/pulsar")
public class SimpleRestController {

  @Value("${pulsar.defaultHost}")
  private String pulsarHost;

  @PostMapping("/pulsar/send")
  public ResponseEntity<String> postToNamedQueueAndWait(
      @RequestParam(value = "writeQueueName", required = true) String writeQueueName,
      @RequestParam(value = "readQueueName", required = true) String readQueueName,
      @RequestBody String queryIn,
      @RequestHeader HttpHeaders headers) {

    PulsarClient client = null;
    Producer producer = null;
    Consumer consumer = null;
    try {
      client = PulsarClient.builder().serviceUrl(pulsarHost).build();
      consumer = client.newConsumer()
          .topic(readQueueName)
          .subscriptionName(this.getClass().getName())
          .consumerName(this.getClass().getName())
          .subscriptionType(SubscriptionType.Shared)
          .subscribe();
//      while (!consumer.hasReachedEndOfTopic()) {
//        // Drain the subscription before we send a new request
//        log.info("Draining topic");
//        consumer.receive();
//      }
//      log.info("Drained topic");

      producer = client.newProducer()
          .topic(writeQueueName)
          .producerName("TestAppPusher")
          .create();

      log.info("Sending request");
      producer.newMessage()
          .properties(filterHeaders(headers))
          .value(queryIn.getBytes())
          .send();
      log.info("Sent request");
      var response = consumer.receive(60, TimeUnit.SECONDS);
      log.info("Received response");
      response.getProperties();
      var responseStr = new String(response.getData());
      log.info(format("Received [{0}]", responseStr));
      consumer.acknowledge(response);

      HttpHeaders outHeaders = new HttpHeaders();
      response.getProperties().forEach((k, vl) -> outHeaders.add((String) k, (String) vl));
      return ResponseEntity.ok().headers(new HttpHeaders(outHeaders)).body(responseStr);
    } catch (PulsarClientException e) {
      log.error("Exception:", e);
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    } finally {
      if (consumer != null) {
        try {
          consumer.unsubscribe();
          consumer.close();
        } catch (PulsarClientException e) {
          log.error("Exception:", e);
        }
        consumer = null;
      }
      if (producer != null) {
        try {
          producer.close();
        } catch (PulsarClientException e) {
          log.error("Exception:", e);
        }
        producer = null;
      }
      if (client != null) {
        try {
          client.close();
        } catch (PulsarClientException e) {
          log.error("Exception:", e);
        }
        client = null;
      }
    }

  }

  private List<String> exclusionHeaders = new ArrayList<>() {
    {
      add("content-length");
      add("Accept");
      add("User-Agent");
      add("Connection");
      add("Postman-Token");
      add("Host");
      add("cache-control");
      add("accept-encoding");
      add("Content-Type");
    }
  };

  private Map<String, String> filterHeaders(HttpHeaders headers) {
    var result = headers.toSingleValueMap().entrySet().stream()
        .filter(v -> !exclusionHeaders.contains(v.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return result;
  }

  @PostMapping("/pulsar/write")
  public ResponseEntity<Void> postToNamedQueue(
      @RequestParam(value = "queueName", required = true) String queueName,
      @RequestBody String queryIn,
      @RequestHeader HttpHeaders headers) {

    PulsarClient client = null;
    Producer producer = null;
    try {
      client = PulsarClient.builder().serviceUrl(pulsarHost).build();

      producer = client.newProducer().topic(queueName).create();

      producer.newMessage()
          .properties(filterHeaders(headers))
          .value(queryIn.getBytes())
          .send();

      return new ResponseEntity<>(HttpStatus.OK);
    } catch (PulsarClientException e) {
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    } finally {
      if (producer != null) {
        try {
          producer.close();
        } catch (PulsarClientException e) {
          e.printStackTrace();
        }
        producer = null;
      }
      if (client != null) {
        try {
          client.close();
        } catch (PulsarClientException e) {
          e.printStackTrace();
        }
        client = null;
      }
    }

  }


}
