package com.github.sioncheng.esp;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class MainApp {

    public static void main(String[] args) throws Exception {
        System.out.println("hello elastic search");

        createClient();

        final String name = "secisland";
        final String type = "secilog";

        if ("createIndex".equalsIgnoreCase(args[0])) {
            createIndex(name, type);
        } else if ("insertDoc".equalsIgnoreCase(args[0])) {
            insertDoc(name, type);
        } else if ("updateDoc".equalsIgnoreCase(args[0])) {
            updateDoc(name, type);
        } else if ("getDoc".equalsIgnoreCase(args[0])) {
            getDoc();
        } else if ("deleteDoc".equalsIgnoreCase(args[0])) {
            deleteDoc();
        } else {
            System.out.println("nothing to do");
        }

        client.close();
    }

    static Client createClient() throws UnknownHostException {
        InetSocketTransportAddress esAddress =
                new InetSocketTransportAddress(InetAddress.getByName(esHostName), 9300);

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "my-application")
                .put("client.transport.sniff", true)
                .build();

         client = TransportClient.builder()
                .settings(settings)
                .build().addTransportAddress(esAddress);

        System.out.println("connect to babaili");

        return client;
    }

    static void createIndex(String indexName, String type) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("settings")
                        .field("number_of_shards", 1).field("number_of_replicas", 0)
                    .endObject()
                .endObject()
                .startObject()
                    .startObject(type)
                        .startObject("properties")
                            .startObject("type")
                                .field("type", "string").field("store","yes")
                            .endObject()
                            .startObject("eventCount")
                                .field("type","long").field("store","yes")
                            .endObject()
                            .startObject("eventDate")
                                .field("type", "date").field("store","yes")
                            .endObject()
                            .startObject("message")
                                .field("type","string").field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName).setSource(mapping);
        CreateIndexResponse response = cirb.execute().actionGet();
        if (response.isAcknowledged()) {
            System.out.println("index created.");
        } else {
            System.out.println("index creation failed.");
        }
    }

    static void insertDoc(String indexName, String type) throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("","").endObject();
        IndexResponse response = client.prepareIndex(indexName, type, "1")
                .setSource(doc).get();

        System.out.println(String.format("index: %s insert doc id: %s", response.getIndex(), response.getId()));
    }

    static void updateDoc(String indexName, String type) throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(indexName);
        updateRequest.type(type);
        updateRequest.id("1");

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("type", "syslog")
                .field("eventCount", 1)
                .field("eventDate", new Date())
                .field("message", "secilog update doc test")
                .endObject();

        updateRequest.doc(doc);

        UpdateResponse response = client.update(updateRequest).get();

        System.out.println(String.format("index: %s update doc id: %s", response.getIndex(), response.getId()));

    }

    static void getDoc() {
        GetResponse response = client.prepareGet("secisland", "secilog", "1").get();
        String source = response.getSource().toString();
        long version = response.getVersion();
        String indexName = response.getIndex();
        String type = response.getType();
        String id = response.getId();

        System.out.println(String.format("source %s version %d indexName %s type %s id %s"
                , source, version, indexName, type, id));
    }

    static void deleteDoc() {
        DeleteResponse deleteResponse = client.prepareDelete("secisland", "secilog", "1").get();
        System.out.println(String.format("deleted ? %s", Boolean.toString(deleteResponse.isFound())));

    }


    private static Client client;

    private static final String esHostName = "babaili";

}
