package de.tu_berlin.dos.arm.archon.phoebe.managers;

import de.tu_berlin.dos.arm.archon.common.utils.CheckedConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class DataManager {

    public record Profile(
            Integer expId, String jobName, int scaleOut, int workload, double avgLat,
            double avgThr, boolean isBckPres, double perBckPres, double avgConsLag, double avgChkDur, double cpuAvg, double cpuTotal,
            double memAvg, double memTotal, Long startTs, Long stopTs) {

        @Override
        public String toString() {
            return "{" +
                    "expId:" + expId +
                    ",jobName:'" + jobName + '\'' +
                    ",scaleOut:" + scaleOut +
                    ",workload:" + workload +
                    ",avgLat:" + avgLat +
                    ",avgThr:" + avgThr +
                    ",isBckPres:" + isBckPres +
                    ",perBckPres:" + perBckPres +
                    ",avgConsLag:" + avgConsLag +
                    ",avgChkDur:" + avgChkDur +
                    ",cpuAvg:" + cpuAvg +
                    ",memAvg:" + memAvg +
                    ",cpuTotal:" + cpuTotal +
                    ",memTotal:" + memTotal +
                    ",startTs:" + startTs +
                    ",stopTs:" + stopTs +
                    '}';
        }
    }

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(DataManager.class);
    private static final String DB_FILE_NAME = "phoebe";

    /******************************************************************************
     * STATIC BEHAVIOURS
     ******************************************************************************/

    public static DataManager create() {

        return new DataManager();
    }

    private static Connection connect() throws Exception {

        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", DB_FILE_NAME));
    }

    private static void executeUpdate(String query) {

        try (Connection conn = connect();
             Statement statement = conn.createStatement()) {

            statement.executeUpdate(query);
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static void executeQuery(String query, CheckedConsumer<ResultSet> callback) {

        try (Connection connection = connect();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {

                callback.accept(resultSet);
            }
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private DataManager() { }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public DataManager initProfiles(int expId, boolean removePrevious) {

        String createTable =
            "CREATE TABLE IF NOT EXISTS profiles " +
            "(expId INTEGER NOT NULL, " +
            "scaleOut INTEGER NOT NULL, " +
            "jobId TEXT NOT NULL, " +
            "workload INTEGER NOT NULL, " +
            "avgLat REAL NOT NULL, " +
            "avgThr REAL NOT NULL, " +
            "isBckPres INTEGER NOT NULL, " +
            "perBckPres REAL NOT NULL, " +
            "avgConsLag REAL NOT NULL, " +
            "avgChkDur REAL NOT NULL, " +
            "cpuAvg REAL NOT NULL, " +
            "memAvg REAL NOT NULL, " +
            "cpuTotal REAL NOT NULL, " +
            "memTotal REAL NOT NULL, " +
            "startTs INTEGER NOT NULL, " +
            "stopTs INTEGER NOT NULL);";
        DataManager.executeUpdate(createTable);
        if (removePrevious) {

            DataManager.executeUpdate(String.format("DELETE FROM profiles WHERE expId = %d;", expId));
        }
        return this;
    }

    public DataManager deleteRow(int expId, String jobId) {

        DataManager.executeUpdate(String.format("DELETE FROM profiles WHERE expId = %d AND jobId = '%s';", expId, jobId));
        return this;
    }

    public static void main(String[] args) {

        DataManager dm = new DataManager();
        dm.deleteRow(3, "1a381c8b6c75a6476fd5b733a3390fea");
    }

    public DataManager addProfile(
            int expId, int scaleOut, String jobId, int workload, Double avgLat, Double avgThr,
            int isBckPres, Double perBckPres, Double avgConsLag, Double avgChkDur, double cpuAvg, double memAvg,
            double cpuTotal, double memTotal, long startTs, long stopTs) {

        String insertValue = String.format(
            "INSERT INTO profiles ( " +
            "expId, scaleOut, jobId, workload, avgLat, avgThr, isBckPres, perBckPres, avgConsLag, " +
            "avgChkDur, cpuAvg, memAvg, cpuTotal, memTotal, startTs, stopTs ) " +
            "VALUES (" +
            "%d, %d, '%s', %d, %f, %f, %d, %f, %f, %f, %f, %f, %f, %f, %d, %d);",
            expId, scaleOut, jobId, workload, avgLat, avgThr, isBckPres, perBckPres, avgConsLag,
            avgChkDur, cpuAvg, memAvg, cpuTotal, memTotal, startTs, stopTs);
        DataManager.executeUpdate(insertValue);
        return this;
    }

    public List<Profile> getProfiles(List<Integer> expIds) {

        List<Profile> profiles = new ArrayList<>();
        String selectValues = String.format(
            "SELECT " +
            "expId, jobId, scaleOut, workload, avgLat, avgThr, isBckPres, perBckPres, " +
            "avgConsLag, avgChkDur, cpuAvg, memAvg, cpuTotal, memTotal, startTs, stopTs " +
            "FROM profiles " +
            "WHERE expId IN (%s) " +
            "ORDER BY scaleOut ASC, workload ASC;",
            expIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        DataManager.executeQuery(selectValues, (rs) -> {
            profiles.add(
                new Profile(
                    rs.getInt("expId"),
                    rs.getString("jobId"),
                    rs.getInt("scaleOut"),
                    rs.getInt("workload"),
                    rs.getDouble("avgLat"),
                    rs.getDouble("avgThr"),
                    rs.getBoolean("isBckPres"),
                    rs.getDouble("perBckPres"),
                    rs.getDouble("avgConsLag"),
                    rs.getDouble("avgChkDur"),
                    rs.getDouble("cpuAvg"),
                    rs.getDouble("memAvg"),
                    rs.getDouble("cpuTotal"),
                    rs.getDouble("memTotal"),
                    rs.getLong("startTs"),
                    rs.getLong("stopTs")));
        });
        return profiles;
    }
}
