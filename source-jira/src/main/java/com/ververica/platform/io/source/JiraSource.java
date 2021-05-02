package com.ververica.platform.io.source;

import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.JiraRestClientFactory;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.Project;
import com.atlassian.jira.rest.client.api.domain.SearchResult;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.ververica.platform.entities.Commit;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Iterator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads commits from a Git repository using JGit and extracts metadata.
 *
 * <p>See https://github.com/centic9/jgit-cookbook for a couple of examples
 */
public class JiraSource extends RichSourceFunction<Commit> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(JiraSource.class);
  public static final int MAX_RESULTS_PER_REQUEST = 100;

  private final String jiraUrl;
  private final String jiraProject;

  private final long pollIntervalMillis;

  private volatile boolean running = true;

  private String lastCommitHash;
  private transient ListState<String> state;
  private transient Path gitRepoDir = null;

  public static void main(String[] args) throws URISyntaxException, IOException {
    JiraSource jira = new JiraSource("https://issues.apache.org/jira", "FLINK", -1);
    jira.open(null);
    jira.run(null);
  }

  /**
   * @param jiraUrl main jira URL
   * @param jiraProject project to follow
   * @param pollIntervalMillis time to wait after processing one bulk of requests
   */
  public JiraSource(String jiraUrl, String jiraProject, long pollIntervalMillis) {
    this.jiraUrl = jiraUrl;
    this.jiraProject = jiraProject;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<Commit> ctx) throws URISyntaxException, IOException {
    final JiraRestClientFactory factory = new AsynchronousJiraRestClientFactory();
    try (JiraRestClient client = factory.create(new URI(jiraUrl), builder -> {})) {
      Project project = client.getProjectClient().getProject(jiraProject).claim();

      int continueAt = 0;
      SearchResult tickets =
          client
              .getSearchClient()
              .searchJql("project = " + jiraProject, MAX_RESULTS_PER_REQUEST, continueAt, null)
              .claim();

      Iterable<Issue> issues = tickets.getIssues();
      for (Issue issue : issues) {
        // TODO
      }

      //      Issue issue = client.getIssueClient().getIssue("FLINK-17704").claim();
      //      System.out.println(issue);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    state.clear();
    state.add(lastCommitHash);
  }

  @Override
  public void initializeState(FunctionInitializationContext ctx) throws Exception {
    state =
        ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("instant", Types.STRING));

    if (ctx.isRestored()) {
      Iterator<String> data = state.get().iterator();
      if (data.hasNext()) {
        lastCommitHash = data.next();
      }
    }
  }

  @Override
  public void open(Configuration configuration) {}

  @Override
  public void close() {}
}
