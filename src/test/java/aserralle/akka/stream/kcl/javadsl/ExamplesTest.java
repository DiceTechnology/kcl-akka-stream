/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import aserralle.akka.stream.kcl.KinesisWorkerCheckpointSettings;
import aserralle.akka.stream.kcl.KinesisWorkerSourceSettings;
import scala.concurrent.duration.FiniteDuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.SimpleRecordsFetcherFactory;
import software.amazon.kinesis.retrieval.polling.SynchronousBlockingRetrievalFactory;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExamplesTest {

    public static void main(String[] args) {

        //#init-client
        Region region = Region.EU_WEST_1;

      DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

      KinesisAsyncClient kinesisClient =
            KinesisAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
        DynamoDbAsyncClient dynamoClient =
            DynamoDbAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
        CloudWatchAsyncClient cloudWatchClient =
            CloudWatchAsyncClient.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .build();
        //#init-client

        //#init-system
        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer materializer = ActorMaterializer.create(system);
        //#init-system

        //#worker-settings
        final KinesisWorkerSource.WorkerBuilder workerBuilder = new KinesisWorkerSource.WorkerBuilder() {
            @Override
            public Scheduler build(ShardRecordProcessorFactory recordProcessorFactory) {
                String streamName = "jh-local-stream";
                ConfigsBuilder configsBuilder =
                    new ConfigsBuilder(
                            streamName,
                        "jh-app-2",
                        kinesisClient,
                        dynamoClient,
                        cloudWatchClient,
                        "shardWorker-" + UUID.randomUUID(),
                        recordProcessorFactory);

                RetrievalConfig retrievalConfig =
                        configsBuilder.retrievalConfig().retrievalFactory(
                                new SynchronousBlockingRetrievalFactory(
                                        streamName,
                                        kinesisClient,
                                        new SimpleRecordsFetcherFactory(),
                                        1000));
                return new Scheduler(
                    configsBuilder.checkpointConfig(),
                    configsBuilder.coordinatorConfig(),
                    configsBuilder.leaseManagementConfig(),
                    configsBuilder.lifecycleConfig(),
                    configsBuilder.metricsConfig(),
                    configsBuilder.processorConfig(),
                        retrievalConfig
                );
            }
        };

        final KinesisWorkerSourceSettings workerSettings = KinesisWorkerSourceSettings.create(
            1000,
            FiniteDuration.apply(10L, TimeUnit.SECONDS),
            FiniteDuration.apply(1L, TimeUnit.MINUTES));
        //#worker-settings

        //#worker-source
//        final Executor workerExecutor = Executors.newFixedThreadPool(100);
//        final Source<CommittableRecord, Scheduler> workerSource =
//            KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor );


    //#worker-source

    //#checkpoint
    final KinesisWorkerCheckpointSettings checkpointSettings =
            KinesisWorkerCheckpointSettings.create(
                1000,
                FiniteDuration.apply(30L, TimeUnit.SECONDS));

    final Executor workerExecutor = Executors.newFixedThreadPool(100);


//      KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor)
//          .runForeach(cr -> {
//              cr.tryToCheckpoint();
//              byte[] barr = new byte[cr.record().data().capacity()];
//              cr.record().data().get(barr);
//              System.out.println("Got data: " + new String(barr));
//
//          }, materializer);

      KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor)
          .alsoTo(
                  Sink.foreach(cr -> {
              byte[] barr = new byte[cr.record().data().capacity()];
              cr.record().data().get(barr);
              System.out.println("Got data: " + new String(barr));
//              return param;
          }))
        .to(KinesisWorkerSource.checkpointRecordsSink(checkpointSettings))
        .run(materializer);

}
    //#checkpoint

}
