package de.tu_berlin.dos.arm.archon.phoebe.managers;

import com.google.gson.*;
import de.tu_berlin.dos.arm.archon.common.clients.flink.FlinkClient;
import de.tu_berlin.dos.arm.archon.common.clients.kubernetes.Helm;
import de.tu_berlin.dos.arm.archon.common.clients.kubernetes.Helm.CommandBuilder;
import de.tu_berlin.dos.arm.archon.common.clients.kubernetes.Helm.CommandBuilder.Command;
import de.tu_berlin.dos.arm.archon.common.clients.kubernetes.K8sClient;
import de.tu_berlin.dos.arm.archon.common.clients.prometheus.PrometheusClient;
import de.tu_berlin.dos.arm.archon.common.data.TimeSeries;
import de.tu_berlin.dos.arm.archon.common.utils.FileReader;
import de.tu_berlin.dos.arm.archon.common.utils.PropertyTree;
import de.tu_berlin.dos.arm.archon.phoebe.execution.Job;
import io.fabric8.chaosmesh.v1alpha1.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ClientsManager implements AutoCloseable {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(ClientsManager.class);

    private static final String BACKPRESSURE = "flink_taskmanager_job_task_isBackPressured";
    private static final String LATENCY = "flink_taskmanager_job_task_operator_myLatencyHistogram";
    private static final String THROUGHPUT = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate";
    private static final String CHECKPOINT_DURATIONS = "flink_jobmanager_job_lastCheckpointDuration";
    private static final String CONSUMER_LAG = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max";
    private static final String CPU_LOAD = "flink_taskmanager_Status_JVM_CPU_Load";
    private static final String MEMORY_USED = "flink_taskmanager_Status_JVM_Memory_Heap_Used";
    private static final String NUM_TASKMANAGERS = "flink_jobmanager_numRegisteredTaskManagers";
    private static final String NUM_RESTARTS = "sum(max_over_time(flink_jobmanager_job_numRestarts[1h]))";
    private static final String UPTIME = "flink_jobmanager_job_uptime";
    private static final String DELAY = "abs(((sum(flink_taskmanager_job_task_operator_currentInputWatermark{operator_name=\"EventFilterBoltViewStream\"}>0) - sum(flink_taskmanager_job_task_operator_currentInputWatermark{operator_name=\"Sink_KafkaSink\"}>0)) / avg(flink_jobmanager_numRegisteredTaskManagers) / 1000)) < 1500";
    private static final String STATE_USAGE = "(sum(flink_taskmanager_job_task_operator_window_contents_rocksdb_block_cache_usage{operator_name=\"CampaignProcessor\"}) + sum(flink_taskmanager_job_task_operator_window_contents_rocksdb_block_cache_pinned_usage{operator_name=\"CampaignProcessor\"})) / (max(flink_taskmanager_job_task_operator_window_contents_rocksdb_block_cache_capacity) * avg(flink_jobmanager_numRegisteredTaskManagers))";

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static ClientsManager create(
            String namespace, String flinkUrl, String jarPath, String sourceRegex, String sinkRegex,
            String savepointPath, String promUrl) throws Exception {

        return new ClientsManager(namespace, flinkUrl, jarPath, sourceRegex, sinkRegex, savepointPath, promUrl);
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    public final String namespace;
    public final K8sClient k8sClient;
    public final FlinkClient flink;
    public final Path jarPath;
    public final String sourceRegex;
    public final String sinkRegex;
    public final String savepointPath;
    public final PrometheusClient prom;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public ClientsManager(
            String namespace, String flinkUrl, String jarPath, String sourceRegex, String sinkRegex,
            String savepointPath, String promUrl) {

        this.namespace = namespace;
        this.k8sClient = new K8sClient();
        this.prom = new PrometheusClient(promUrl, gson);
        this.flink = new FlinkClient(flinkUrl, gson);
        this.jarPath = Paths.get(jarPath);
        this.sourceRegex = sourceRegex;
        this.sinkRegex = sinkRegex;
        this.savepointPath = savepointPath;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void createNamespace() throws Exception {

        this.k8sClient.deleteNamespace(this.namespace);
        this.k8sClient.createOrReplaceNamespace(this.namespace);
    }

    public void deleteNamespace() throws Exception {

        this.k8sClient.deleteNamespace(this.namespace);
    }

    public void deployHelm(PropertyTree deployment) throws Exception {

        LOG.info("Helm deploy: " + deployment);
        CommandBuilder builder = CommandBuilder.builder();
        builder.setCommand(Command.INSTALL);
        builder.setName(deployment.key);
        if (deployment.exists("chart")) {

            String path = FileReader.GET.path(deployment.find("chart").value);
            builder.setChart(path);
        }
        else builder.setChart(deployment.key);
        if (deployment.exists("repo")) builder.setFlag("--repo", deployment.find("repo").value);
        if (deployment.exists("values")) {

            String path = "/home/zlatina/projects/BA/k8s-setup/archon-master/phoebe/src/main/resources/" + deployment.find("values").value;
            builder.setFlag("--values", path);
        }
        if (deployment.exists("version")) builder.setFlag("--version", deployment.find("version").value);
        builder.setNamespace(this.namespace);
        Helm.get.execute(builder.build());
    }

    public void deployYaml(PropertyTree deployment) throws Exception {

        LOG.info("Yaml deploy: " + deployment);
        deployment.forEach(file -> this.k8sClient.createOrReplaceResources(this.namespace, file.value));
    }

    public List<String> getPodNames(String labelKey, String labelVal) {

        return k8sClient.getPods(this.namespace, labelKey, labelVal);
    }

    public String uploadJar() throws Exception {

        JsonObject json = this.flink.uploadJar(this.jarPath);
        String[] parts = json.get("filename").getAsString().split("/");
        return parts[parts.length - 1];
    }

    public List<String> getJobs() throws Exception {

        JsonObject json = this.flink.getJobs();
        List<String> jobs = new ArrayList<>();
        json.get("jobs").getAsJsonArray().forEach(e -> {

            jobs.add(e.getAsJsonObject().get("id").getAsString());
        });
        return jobs;
    }

    public String startJob(String jarId, JsonObject programArgs) throws Exception {

        JsonObject json = this.flink.startJob(jarId, programArgs);
        return json.get("jobid").getAsString();
    }

    public String rescaleJob(String jobId, JsonObject programArgsList) throws Exception {

        // Initiate job save
        AtomicReference<String> location = new AtomicReference<>();
        JsonObject body = new JsonObject();
        body.addProperty("target-directory", this.savepointPath);
        body.addProperty("cancel-job", false);
        int attempts = 0;
        while (true) {
            LOG.info("Saving job");
            String requestId = this.flink.saveJob(jobId, body).get("request-id").getAsString();
            // wait until save is completed
            JsonObject response = this.flink.checkStatus(jobId, requestId);
            while (!"COMPLETED".equals(response.get("status").getAsJsonObject().get("id").getAsString())) {

                new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                response = this.flink.checkStatus(jobId, requestId);
            }
            // ensure save completed successfully
            if (response.has("operation") &&
                response.get("operation").getAsJsonObject().has("location")) {
                LOG.info("Save complete");
                // retrieve savepoint location and stop job
                location.set(response.get("operation").getAsJsonObject().get("location").getAsString());
                this.stopJob(jobId);
                break;
            }
            else attempts++;
            if (attempts > 60) throw new IllegalStateException("Unable to rescale, throwing exception");
            new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
        }
        LOG.info("Restarting job from savepoint with:" + programArgsList.toString());
        // restart job with new scaleout
        programArgsList.addProperty("savepointPath", location.get());
        return this.flink.startJob(Job.jarId, programArgsList).get("jobid").getAsString();
    }

    public boolean stopJob(String jobId) throws Exception {

        return this.flink.stopJob(jobId);
    }

    public List<String> getVertices(String jobId) throws Exception {

        JsonObject response = this.flink.getVertices(jobId);
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        List<String> operatorIds = new ArrayList<>();
        nodes.forEach(vertex -> operatorIds.add(vertex.getAsJsonObject().get("id").getAsString()));
        return operatorIds;
    }

    public Set<String> getTaskManagers(String jobId) throws Exception {

        List<String> vertices = this.getVertices(jobId);
        Set<String> taskManagers = new HashSet<>();
        for (String id : vertices) {

            JsonObject response = this.flink.getTaskManagers(jobId, id);
            JsonArray arr = response.getAsJsonArray("taskmanagers");
            arr.forEach(taskManager -> taskManagers.add(taskManager.getAsJsonObject().get("taskmanager-id").getAsString()));
        }
        return taskManagers;
    }

    public String getSinkId(String jobId) throws Exception {

        JsonObject response = this.flink.getVertices(jobId);
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        for (JsonElement node : nodes) {

            String description = node.getAsJsonObject().get("description").getAsString();
            if (description.matches(this.sinkRegex)) return node.getAsJsonObject().get("id").getAsString();
        }
        throw new IllegalStateException("Unable to find sink operator for job with ID " + jobId);
    }

    public long getTs(String jobId, String state) throws Exception {

        return (long) Math.ceil(this.flink.getJob(jobId).get("timestamps").getAsJsonObject().get(state).getAsLong() / 1000.0);
    }

    public long getLatestTs(String jobId) throws Exception {

        long now = this.flink.getLatestTs(jobId).get("now").getAsLong();
        return (long) Math.ceil(now / 1000.0);
    }

    public int getScaleOut(String jobId) throws Exception {

        JsonObject response = this.flink.getJob(jobId);
        return response.get("vertices").getAsJsonArray().get(0).getAsJsonObject().get("parallelism").getAsInt();
    }

    public void injectFailure(String taskManager) throws Exception {

        this.k8sClient.execCommandOnPod(taskManager, this.namespace, "sh", "-c", "kill 1");
    }

    public NetworkChaos injectDelay(String taskManager) {

        String name = "delay-" + taskManager + "-" + RandomStringUtils.random(10, true, true);
        DelaySpec delay = new DelaySpecBuilder().withLatency("60000ms").withCorrelation("100").withJitter("0ms").build();
        return this.k8sClient.createOrReplaceNetworkChaos(this.namespace, new NetworkChaosBuilder()
            .withNewMetadata().withName(name).endMetadata()
            .withNewSpec()
            .withAction("delay")
            .withMode("one")
            .withNewSelector().withFieldSelectors(Map.of("metadata.name", taskManager)).endSelector()
            .withDelay(delay)
            .withDuration("60s")
            .endSpec()
            .build());
    }

    public TimeSeries getBckPres(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",task_name=~\"%s\",namespace=\"%s\"})",
            BACKPRESSURE, jobName, this.sourceRegex, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getLat(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})/" +
            "count(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})",
            LATENCY, jobName, this.namespace, LATENCY, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getThr(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",namespace=\"%s\"})",
            THROUGHPUT, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getChkDur(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            CHECKPOINT_DURATIONS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getConsLag(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",namespace=\"%s\"})/count(%s{job_name=\"%s\",namespace=\"%s\"})",
            CONSUMER_LAG, jobName, this.namespace, CONSUMER_LAG, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getCpuLoad(String taskManager, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=\"%s\",namespace=\"%s\"}",
            CPU_LOAD, taskManager, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getMemUsed(String taskManager, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=\"%s\",namespace=\"%s\"}",
            MEMORY_USED, taskManager, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getNumWorkers(long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            NUM_TASKMANAGERS, this.namespace);
        LOG.info(query);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getStateMem(long startTs, long stopTs) throws Exception {

        return this.prom.queryRange(STATE_USAGE, startTs, stopTs);
    }

    public TimeSeries getDelay(long startTs, long stopTs) throws Exception {

        return this.prom.queryRange(DELAY, startTs, stopTs);
    }

    public TimeSeries getNumRestarts(long startTs, long stopTs) throws Exception {

        return this.prom.queryRange(NUM_RESTARTS, startTs, stopTs);
    }

    public long getUptime(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            UPTIME, jobName, this.namespace);

        TimeSeries ts = this.prom.queryRange(query, startTs, stopTs);
        return (ts.getLast() != null && ts.getLast().value != null) ? ts.getLast().value.longValue() / 1000 : 0;
    }

    public Map<String, TimeSeries> getAllCpuLoad(String metric, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            CPU_LOAD, this.namespace);
        return this.prom.queryRange(query, metric, startTs, stopTs);
    }

    public Map<String, TimeSeries> getAllMemUsed(String metric, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            MEMORY_USED, this.namespace);
        return this.prom.queryRange(query, metric, startTs, stopTs);
    }

    @Override
    public void close() throws Exception {

        this.k8sClient.close();
    }
}
