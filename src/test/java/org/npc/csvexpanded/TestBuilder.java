package org.npc.csvexpanded;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBuilder {
    private Reader test_reader;
    private Map<String, String> option_map;
    private DataSourceOptions options;

    @Test
    public void testPath() {
        option_map = new HashMap<>();
        option_map.put("bucket", "test_bucket");
        option_map.put("path", "test_path/");
        option_map.put("schema", "{\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"metadata\": {},\n" +
                "            \"name\": \"name\",\n" +
                "            \"nullable\": true,\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"metadata\": {},\n" +
                "            \"name\": \"id\",\n" +
                "            \"nullable\": true,\n" +
                "            \"type\": \"integer\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"type\": \"struct\"\n" +
                "}");
        options = new DataSourceOptions(option_map);
        test_reader = new Reader(options);
        assertEquals("test_path/", test_reader.path);
    }

    @Test
    public void testBucket() {
        option_map = new HashMap<>();
        option_map.put("bucket", "test_bucket");
        option_map.put("path", "test_path/");
        option_map.put("schema", "{\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"metadata\": {},\n" +
                "            \"name\": \"name\",\n" +
                "            \"nullable\": true,\n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"metadata\": {},\n" +
                "            \"name\": \"id\",\n" +
                "            \"nullable\": true,\n" +
                "            \"type\": \"integer\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"type\": \"struct\"\n" +
                "}");
        options = new DataSourceOptions(option_map);
        test_reader = new Reader(options);
        assertEquals("test_bucket", test_reader.bucket);
    }


}
