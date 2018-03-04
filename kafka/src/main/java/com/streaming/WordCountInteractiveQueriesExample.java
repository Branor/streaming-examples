/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.streaming;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

public class WordCountInteractiveQueriesExample {

  static final String TEXT_LINES_TOPIC = "TextLinesTopic";

  public static void main(String[] args) throws Exception {

    final int port = args.length > 0 ? Integer.valueOf(args[0]) : 1112;
    final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-queries-example");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + port);
    final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

    final KafkaStreams streams = createStreams(streamsConfiguration);
    streams.cleanUp();
    streams.start();

    // Start the Restful proxy for servicing remote access to state stores
    final WordCountInteractiveQueriesRestService restService = startRestProxy(streams, port);

    System.out.println("Rest proxy listening on port: " + port);

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        streams.close();
        restService.stop();
      } catch (Exception e) {
        // ignored
      }
    }));
  }


  static WordCountInteractiveQueriesRestService startRestProxy(final KafkaStreams streams, final int port)
      throws Exception {
    final WordCountInteractiveQueriesRestService
        wordCountInteractiveQueriesRestService = new WordCountInteractiveQueriesRestService(streams);
    wordCountInteractiveQueriesRestService.start(port);
    return wordCountInteractiveQueriesRestService;
  }

  static KafkaStreams createStreams(final Properties streamsConfiguration) {
    final Serde<String> stringSerde = Serdes.String();
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String>
        textLines = builder.stream(stringSerde, stringSerde, TEXT_LINES_TOPIC);

    final KGroupedStream<String, String> groupedByWord = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .groupBy((key, word) -> word, stringSerde, stringSerde);

    // Create a State Store for with the all time word count
    groupedByWord.count("word-count");

    // Create a Windowed State Store that contains the word count for every
    // 1 minute
    groupedByWord.count(TimeWindows.of(60000), "windowed-word-count");

    return new KafkaStreams(builder, streamsConfiguration);
  }

}
