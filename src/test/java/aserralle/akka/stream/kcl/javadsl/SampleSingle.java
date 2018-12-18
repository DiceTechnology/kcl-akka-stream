package aserralle.akka.stream.kcl.javadsl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class SampleSingle {

  private static final Logger log = LoggerFactory.getLogger(SampleSingle.class);

  public static void main(String... args) {
//    if (args.length < 1) {
//      log.error("At a minimum stream name is required as the first argument. Region may be specified as the second argument");
//      System.exit(1);
//    }
//
//    String streamName = args[0];
//    String region = null;
//    if (args.length > 1) {
//      region = args[1];
//    }

    new SampleSingle("jh-local-stream", "eu-west-1").run();
  }

  private final String streamName;
  private final Region region;
  private final KinesisAsyncClient kinesisClient;

  private SampleSingle(String streamName, String region) {
    this.streamName = streamName;
    this.region = Region.of(ObjectUtils.firstNonNull(region, "eu-west-1"));
    this.kinesisClient = KinesisAsyncClient.builder().region(this.region).build();
  }

  private void run() {
//    ScheduledExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
//    ScheduledFuture<?> producerFuture = producerExecutor
//        .scheduleAtFixedRate(this::publishRecord, 10, 1, TimeUnit.SECONDS);

    DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
    ConfigsBuilder configsBuilder =
        new ConfigsBuilder(streamName, "jh-app", kinesisClient, dynamoClient, cloudWatchClient,
            UUID.randomUUID().toString(), new SampleRecordProcessorFactory());

    Scheduler scheduler = new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        configsBuilder.retrievalConfig()
    );

    Thread schedulerThread = new Thread(scheduler);
    schedulerThread.setDaemon(true);
    schedulerThread.start();

    System.out.println("Press enter to shutdown");
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    try {
      reader.readLine();
    } catch (IOException ioex) {
      log.error("Caught exception while waiting for confirm.  Shutting down", ioex);
    }

//    log.info("Cancelling producer, and shutting down excecutor.");
//    producerFuture.cancel(true);
//    producerExecutor.shutdownNow();

    Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
    log.info("Waiting up to 20 seconds for shutdown to complete.");
    try {
      gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.info("Interrupted while waiting for graceful shutdown. Continuing.");
    } catch (ExecutionException e) {
      log.error("Exception while executing graceful shutdown.", e);
    } catch (TimeoutException e) {
      log.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
    }
    log.info("Completed, shutting down now.");
  }


  private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {

    public ShardRecordProcessor shardRecordProcessor() {
      return new SampleRecordProcessor();
    }
  }


  private static class SampleRecordProcessor implements ShardRecordProcessor {

    private static final String SHARD_ID_MDC_KEY = "ShardId";

    private static final Logger log = LoggerFactory
        .getLogger(SampleRecordProcessor.class);

    private String shardId;

    public void initialize(InitializationInput initializationInput) {
      shardId = initializationInput.shardId();
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Processing {} record(s)", processRecordsInput.records().size());
        processRecordsInput.records().forEach(r -> log
            .info("Processing record pk: {} -- Seq: {}", r.partitionKey(), r.sequenceNumber()));
      } catch (Throwable t) {
        log.error("Caught throwable while processing records.  Aborting");
        Runtime.getRuntime().halt(1);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void leaseLost(LeaseLostInput leaseLostInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Lost lease, so terminating.");
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void shardEnded(ShardEndedInput shardEndedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Reached shard end checkpointing.");
        shardEndedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at shard end.  Giving up", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }

    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      MDC.put(SHARD_ID_MDC_KEY, shardId);
      try {
        log.info("Scheduler is shutting down, checkpointing.");
        shutdownRequestedInput.checkpointer().checkpoint();
      } catch (ShutdownException | InvalidStateException e) {
        log.error("Exception while checkpointing at requested shutdown.  Giving up", e);
      } finally {
        MDC.remove(SHARD_ID_MDC_KEY);
      }
    }
  }

}
