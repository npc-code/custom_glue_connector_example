package org.npc.csvexpanded;

/*
 * Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A simple Spark DataSource V2 reads from s3 csv files.
 */
public class CSVExpanded implements DataSourceV2, ReadSupport, WriteSupport {

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        return new Reader(options);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(
            String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        return Optional.of(new Writer(options));
    }

}

class Reader implements DataSourceReader {
    // Assuming the csv files to be read has a fixed schema on premise with two integer columns name, id.
    //TODO: let's attempt to pass in a dynamic schema.
    private final StructType schema = new StructType()
            .add("name", "string")
            .add("id", "int");
    private String bucket, path;

    Reader(DataSourceOptions options) {
        bucket = options.get("bucket").get();
        path = options.get("path").get();
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        AmazonS3 s3Client = AmazonS3Client.builder()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(new DefaultAwsRegionProviderChain().getRegion())
                .withForceGlobalBucketAccessEnabled(true)
                .build();

        List<S3ObjectSummary> my_list = s3Client.listObjects("npc-custom-conn-test-vendor-bucket").getObjectSummaries();
        List<InputPartition<InternalRow>> rows = new ArrayList<>();
        for (int i = 0; i < my_list.size(); i++) {
            //all parameters passed here must be serializable
            rows.add(new JavaSimpleInputPartition(my_list.get(i).getKey(), bucket));
        };

        return rows;

    }
}

class JavaSimpleInputPartition implements InputPartition<InternalRow> {

    private String key, bucket;

    JavaSimpleInputPartition(String key, String bucket) {
        this.key = key;
        this.bucket = bucket;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        return new JavaSimpleInputPartitionReader(key, bucket);
    }

}


class JavaSimpleInputPartitionReader implements InputPartitionReader<InternalRow>  {
    BufferedReader reader;
    String line = null;

    JavaSimpleInputPartitionReader(String key, String bucket) {
        AmazonS3 s3Client = AmazonS3Client.builder()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(new DefaultAwsRegionProviderChain().getRegion())
                .withForceGlobalBucketAccessEnabled(true)
                .build();

        S3Object s3Object = s3Client
                .getObject(new GetObjectRequest(bucket, key));
        reader  = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
    }

    @Override
    public boolean next() {
        try {
            line = reader.readLine();
            return line != null;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalRow get() {
        String[] fields = line.split(",");
        //this will need to be modified to react to the schema passed.
        return new GenericInternalRow(new Object[] {UTF8String.fromString(fields[0]), Integer.parseInt(fields[1])});
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}

class Writer implements DataSourceWriter {
    private DataSourceOptions options;

    Writer(DataSourceOptions options) {
        this.options = options;
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new JavaSimpleDataWriterFactory(options);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) { }

    @Override
    public void abort(WriterCommitMessage[] messages) { }
}

class JavaSimpleDataWriterFactory implements DataWriterFactory<InternalRow> {
    private String bucket, keyPrefix;

    JavaSimpleDataWriterFactory(DataSourceOptions options) {
        bucket = options.get("bucket").get();
        keyPrefix = options.get("keyPrefix").get();
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new JavaSimpleDataWriter(partitionId, taskId, epochId, bucket, keyPrefix);
    }
}

class JavaSimpleDataWriter implements DataWriter<InternalRow> {
    private int partitionId;
    private long taskId;
    private long epochId;
    private String bucket;
    private String keyPrefix;
    private StringBuilder content = new StringBuilder();
    private AmazonS3 s3Client = AmazonS3Client.builder()
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .withRegion(new DefaultAwsRegionProviderChain().getRegion())
            .withForceGlobalBucketAccessEnabled(true)
            .build();

    JavaSimpleDataWriter(int partitionId, long taskId, long epochId, String bucket, String keyPrefix) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.epochId = epochId;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        content.append(record.getInt(0) + "," + record.getInt(1));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        s3Client.putObject(bucket,  keyPrefix + "/" + partitionId,  content.toString());
        return null;
    }

    @Override
    public void abort() throws IOException { }
}
