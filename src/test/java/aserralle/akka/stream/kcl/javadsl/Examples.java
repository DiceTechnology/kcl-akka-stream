/*
 * Copyright (C) 2018 Albert Serrallé
 */

package aserralle.akka.stream.kcl.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import aserralle.akka.stream.kcl.KinesisWorkerCheckpointSettings;
import aserralle.akka.stream.kcl.KinesisWorkerSourceSettings;
import aserralle.akka.stream.kcl.CommittableRecord;
import io.reactivex.Scheduler.Worker;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class Examples {

    //#init-client
    final KinesisAsyncClient amazonKinesisAsync =
        KinesisAsyncClient.builder().region(Region.AWS_GLOBAL).build()
    //#init-client

    //#init-system
    final ActorSystem system = ActorSystem.create();
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    //#init-system

    //#worker-settings
    final KinesisWorkerSource.WorkerBuilder workerBuilder = new KinesisWorkerSource.WorkerBuilder() {
        @Override
        public Scheduler build(ShardRecordProcessorFactory recordProcessorFactory) {
            return new Scheduler()
                    .recordProcessorFactory(recordProcessorFactory)
                    .config(new KinesisClientLibConfiguration(
                            "myApp",
                            "myStreamName",
                            DefaultAWSCredentialsProviderChain.getInstance(),
                            "workerId"
                    ));
        }
    };
    final KinesisWorkerSourceSettings workerSettings = KinesisWorkerSourceSettings.create(
            1000,
            FiniteDuration.apply(1L, TimeUnit.SECONDS), FiniteDuration.apply(1L, TimeUnit.MINUTES));
    //#worker-settings

    //#worker-source
    final Executor workerExecutor = Executors.newFixedThreadPool(100);
    final Source<CommittableRecord, Scheduler> workerSource =
        KinesisWorkerSource.create(workerBuilder, workerSettings, workerExecutor );
    //#worker-source

    //#checkpoint
    final KinesisWorkerCheckpointSettings checkpointSettings = KinesisWorkerCheckpointSettings.create(1000, FiniteDuration.apply(30L, TimeUnit.SECONDS));
    final Flow<CommittableRecord, KinesisClientRecord, NotUsed> checkpointFlow =
        KinesisWorkerSource.checkpointRecordsFlow(checkpointSettings);
    //#checkpoint

}
