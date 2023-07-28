package com.mysql.java.test;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.mysql.java.CDCUtils.*;

@Slf4j
public class test9_1 {

    /**
     * mysql
     */

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("MYSQL数据库CDC增量");

    public static void incrementData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                                     String originalUsername, String originalPassword,
                                     String targetDatabaseType, String targetDbname, String targetSchema, String targetIp, String targetPort,
                                     String targetUsername, String targetPassword,
                                     String tableList, //表名，多表以逗号隔开，//
                                     String kafkaipport,
                                     String topic,
                                     String index, //索引字段名 ipid,pid 复合索引以逗号隔开
                                     String indexName,
                                     String etlTime) throws Exception //索引名称
    {


        KettleEnvironment.init();
        DatabaseMeta originalDbmeta = null; //
        DatabaseMeta targetDbmeta = null; //


        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaipport);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        List<String> fileList = new ArrayList<>();
        fileList.add("D:\\Debezium\\offset\\mysql\\file.dat");
        fileList.add("D:\\Debezium\\offset\\mysql\\dbhistory.dat");
        try {
            for (String s : fileList) {
                File file = new File(s);
                if (file.createNewFile()) {
                    System.out.println("File created: " + file.getName());
                } else {
                    System.out.println("File already exists.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        Configuration config = Configuration.create()
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("database.hostname", originalIp)
                .with("database.port", 3306)
                .with("database.user", originalUsername)
                .with("database.password", originalPassword)
                .with("database.dbname", originalDbname)
                .with("database.server.name", "my-mysql-server1")
                .with("table.include.list", originalSchema + "." + tableList)
                .with("include.schema.changes", "false")
                .with("name", "my-connector-mysql-1")
                .with("offset.storage", FileOffsetBackingStore.class.getName())
                .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\mysql\\\\file.dat")
                .with("offset.flush.interval.ms", 6000)
                .with("database.history", FileDatabaseHistory.class.getName())
                .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\mysql\\\\dbhistory.dat")
                .with("database.history.kafka.bootstrap.servers", kafkaipport)
                .with("database.history.kafka.topic", topic)
                .with("logger.level", "DEBUG")
                .with("database.connection.timezone", "+08:00")
                .build();

        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(record -> {
                    String key = String.valueOf(record.key());

                    Struct structValue = (Struct) record.value();

                    commonCrud(structValue,tableList, key, topic, props, targetSchema, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime,index);

                })
                .build();

        // 启动 engine
        engine.run();

    }

}






