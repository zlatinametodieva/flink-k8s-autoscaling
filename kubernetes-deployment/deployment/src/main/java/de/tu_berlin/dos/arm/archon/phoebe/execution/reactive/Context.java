package de.tu_berlin.dos.arm.archon.phoebe.execution.reactive;

import de.tu_berlin.dos.arm.archon.common.utils.EventTimer;
import de.tu_berlin.dos.arm.archon.common.utils.FileReader;
import de.tu_berlin.dos.arm.archon.common.utils.OrderedProperties;
import de.tu_berlin.dos.arm.archon.common.utils.PropertyTree;
import de.tu_berlin.dos.arm.archon.phoebe.execution.Job;
import de.tu_berlin.dos.arm.archon.phoebe.managers.ClientsManager;
import de.tu_berlin.dos.arm.archon.phoebe.managers.DataManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class Context implements AutoCloseable {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(Context.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static Context create(String propertiesFile) throws Exception {

        return new Context(propertiesFile);
    }

    public static List<Integer> calcRange(int max, int min, int count) {

        List<Integer> range = new ArrayList<>();
        int step = (int) (((max - min) * 1.0 / (count - 1)) + 0.5);
        Stream.iterate(min, i -> i + step).limit(count).forEach(range::add);
        return range;
    }

    /******************************************************************************
     * INSTANCE VARIABLES
     ******************************************************************************/

    public final int expId;
    public final int expLen;
    public final String dataPath;
    public final PropertyTree k8s;
    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final String checkpointInterval;
    public final String windowType;
    public final String windowSize;
    public final String applyJoin;
    public final String applyAggregate;
    public final Job job;
    public final List<Integer> failures = new ArrayList<>();

    public final DataManager dataManager;
    public final ClientsManager clientsManager;
    public final EventTimer eventTimer;

    public final ExecutorService executor = Executors.newFixedThreadPool(10);

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private Context(String propertiesFile) throws Exception {

        OrderedProperties props = FileReader.GET.read(propertiesFile, OrderedProperties.class);
        // get general properties
        this.expId = Integer.parseInt(props.getProperty("general.expId"));
        this.expLen = Integer.parseInt(props.getProperty("general.expLen"));
        this.dataPath = props.getProperty("general.dataPath");
        // ProgramArgs
        this.brokerList = props.getProperty("general.brokerList");
        this.consumerTopic = props.getProperty("general.consumerTopic");
        this.producerTopic = props.getProperty("general.producerTopic");
        this.checkpointInterval = props.getProperty("general.checkpointInterval");
        this.windowType = props.getProperty("general.windowType");
        this.windowSize = props.getProperty("general.windowSize");
        this.applyJoin = props.getProperty("general.applyJoin");
        this.applyAggregate = props.getProperty("general.applyAggregate");
        String entryClass = props.getProperty("general.entryClass");
        this.job = new Job("reactive", this.brokerList, this.consumerTopic, this.producerTopic, this.checkpointInterval,
                    this.windowType, this.windowSize, this.applyJoin, this.applyAggregate, entryClass);

        // configure failures
        int numFailures = Integer.parseInt(props.getProperty("general.numFailures"));
        failures.addAll(calcRange(this.expLen - 1200, 1200, numFailures));

        // get kubernetes properties
        this.k8s = props.getPropertyList("k8s");
        // create clients manager
        String namespace = this.k8s.find("namespace").value;
        String flinkBaseUrl = props.getProperty("clients.flink");
        String jarPath = props.getProperty("general.jarPath");
        String sourceRegex = props.getProperty("general.sourceRegex");
        String sinkRegex = props.getProperty("general.sinkRegex");
        String targetDirectory = props.getProperty("general.targetDirectory"); // rescaleJob
        String promBaseUrl = props.getProperty("clients.prometheus");
        this.clientsManager = ClientsManager.create(
                namespace, flinkBaseUrl, jarPath, sourceRegex,
                sinkRegex, targetDirectory, promBaseUrl);
        // create data manager
        this.dataManager = DataManager.create();
        // create timer manager used in profiling
        this.eventTimer = new EventTimer();

    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void close() throws Exception {

        this.clientsManager.close();
    }
}
