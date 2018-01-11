package com.github.sioncheng.esp;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class MainApp {

    public static void main(String[] args) throws Exception {
        System.out.println("hello elastic search");

        createClient();

        createIndex("secisland", "secilog");

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

    static void createIndex(String type, String indexName) throws IOException {
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

    private static Client client;

    private static final String esHostName = "babaili";

}
