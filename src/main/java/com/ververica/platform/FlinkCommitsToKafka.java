package com.ververica.platform;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ververica.platform.entities.Commit;
import com.ververica.platform.io.source.GithubCommitSource;

import java.time.Instant;
import java.time.LocalDate;

import static com.ververica.platform.io.source.GithubCommitSource.EVALUATION_ZONE;

public class FlinkCommitsToKafka {

  public static final String APACHE_FLINK_REPOSITORY = "apache/flink";

  public static void main(String[] args) {
    ParameterTool params = ParameterTool.fromArgs(args);

    // Sink
    String kafkaServer = params.get("kafka-server", "kafka.vvp.svc");
    String kafkaTopic = params.get("kafka-topic", "flink-commits");

    // Source
    long delayBetweenQueries = params.getLong("poll-interval-ms", 10_000L);
    String startDateString = params.get("start-date", "");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings =
            EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    env.getConfig().enableObjectReuse();

    DataStream<Commit> commits =
        env.addSource(getGithubCommitSource(delayBetweenQueries, startDateString))
            .name("flink-commit-source")
            .uid("flink-commit-source");

    tableEnv.executeSql(
            "CREATE TABLE commits ("
                    + "author STRING, "
                    + "`authorDate` TIMESTAMP(3), "
                    + "commitDate TIMESTAMP(3), "
                    + "committer STRING, "
                    + "filesChanged ARRAY<ROW<filename STRING, linesAdded INT, linesChanged INT, linesRemoved INT>> "
                    + ") WITH ("
                    + "'connector' = 'kafka', "
                    + "'topic' = '" + kafkaTopic + "', "
                    + "'properties.bootstrap.servers' = '" + kafkaServer + "', "
                    + "'format' = 'json'"
                    + ")");

    tableEnv.fromDataStream(commits).executeInsert("commits");
  }

  private static GithubCommitSource getGithubCommitSource(
      final long delayBetweenQueries, final String startDateString) {
    final GithubCommitSource githubCommitSource;
    if (startDateString.isEmpty()) {
      githubCommitSource =
          new GithubCommitSource(APACHE_FLINK_REPOSITORY, Instant.now(), delayBetweenQueries);
    } else {
      Instant startDate =
          LocalDate.parse(startDateString).atStartOfDay(EVALUATION_ZONE).toInstant();
      githubCommitSource =
          new GithubCommitSource(APACHE_FLINK_REPOSITORY, startDate, delayBetweenQueries);
    }
    return githubCommitSource;
  }
}
