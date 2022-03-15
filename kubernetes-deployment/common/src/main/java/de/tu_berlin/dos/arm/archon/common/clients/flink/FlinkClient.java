package de.tu_berlin.dos.arm.archon.common.clients.flink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.tu_berlin.dos.arm.archon.common.clients.flink.responses.*;
import de.tu_berlin.dos.arm.archon.common.utils.FileReader;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.logging.HttpLoggingInterceptor;
import okhttp3.logging.HttpLoggingInterceptor.Level;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlinkClient {

    public final String baseUrl;
    public final FlinkRest service;
    HttpLoggingInterceptor logging = new HttpLoggingInterceptor();

    public FlinkClient(String baseUrl, Gson gson) {

        //logging.setLevel(Level.HEADERS);
        this.baseUrl = "http://" + baseUrl + "/";
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.MINUTES)
            .readTimeout(100, TimeUnit.MINUTES)
            .writeTimeout(100, TimeUnit.MINUTES)
            //.addInterceptor(logging)
            .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        this.service = retrofit.create(FlinkRest.class);
    }

    public JsonObject uploadJar(Path jarPath) throws Exception {

        File file = jarPath.toAbsolutePath().toFile();
        String contentType = "Content-Type: application/x-java-archive";
        RequestBody requestFile = RequestBody.create(MediaType.parse(contentType), file);
        MultipartBody.Part body = MultipartBody.Part.createFormData("jarfile", file.getName(), requestFile);
        Response<JsonObject> response = this.service.uploadJar(body).execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(
            String.format("Flink error: %s", response.errorBody().string()));
    }

    public JsonObject getJobs() throws IOException {

        Response<JsonObject> response = this.service.getJobs().execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(
                String.format("Flink error: %s", response.errorBody().string()));
    }

    public JsonObject startJob(String jarId, JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.startJob(jarId, body).execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(String.format("Flink error: %s", response.errorBody().string()));
    }

    public boolean stopJob(String jobId) throws IOException {

        Response<Void> response = this.service.stopJob(jobId).execute();
        if (response.isSuccessful()) return true;
        else throw new IllegalStateException(String.format("Flink error: %s", response.errorBody().string()));
    }

    public JsonObject saveJob(String jobId, JsonObject body) throws IOException {

        return this.service.saveJob(jobId, body).execute().body();
    }

    public JsonObject checkStatus(String jobId, String requestId) throws IOException {

        return this.service.checkStatus(jobId, requestId).execute().body();
    }

    public JsonObject getVertices(String jobId) throws IOException {

        return this.service.getVertices(jobId).execute().body();
    }

    public JsonObject getTaskManagers(String jobId, String vertexId) throws IOException {

        return this.service.getTaskManagers(jobId, vertexId).execute().body();
    }

    public JsonObject getLatestTs(String jobId) throws IOException {

        return this.service.getLatestTs(jobId).execute().body();
    }

    public Checkpoints getCheckpoints(String jobId) throws IOException {

        return this.service.getCheckpoints(jobId).execute().body();
    }

    public JsonObject getJob(String jobId) throws IOException {

        Response<JsonObject> response = this.service.getJob(jobId).execute();
        if (response.isSuccessful()) return response.body();
        else throw new IllegalStateException(String.format("Flink error: %s", response.errorBody().string()));

    }

    public static void main(String[] args) throws Exception {

        FlinkClient client = new FlinkClient("130.149.248.64:30081", new GsonBuilder().disableHtmlEscaping().serializeNulls().create());

        System.out.println(client.getVertices("f5b85040532bba760320f527d54dbdee"));

        JsonObject response = client.getVertices("f5b85040532bba760320f527d54dbdee");
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        List<String> operatorIds = new ArrayList<>();
        nodes.forEach(vertex -> {

            operatorIds.add(vertex.getAsJsonObject().get("id").getAsString());
        });
        System.out.println(Arrays.toString(operatorIds.toArray()));

        Set<String> taskManagers = new HashSet<>();
        for (String id : operatorIds) {

            response = client.getTaskManagers("f5b85040532bba760320f527d54dbdee", id);
            JsonArray arr = response.getAsJsonArray("taskmanagers");
            arr.forEach(taskManager -> {

                taskManagers.add(taskManager.getAsJsonObject().get("taskmanager-id").getAsString());
            });
        }
        System.out.println(Arrays.toString(taskManagers.toArray()));

        response = client.getVertices("f5b85040532bba760320f527d54dbdee");
        nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        nodes.forEach(node -> {

            if (node.getAsJsonObject().get("description").getAsString().matches("Sink.*")) System.out.println("here");
        });
    }
}
