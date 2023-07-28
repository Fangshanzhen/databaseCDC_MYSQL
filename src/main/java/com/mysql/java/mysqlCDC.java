package com.mysql.java;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.mysql.java.CDCUtils.*;
import static com.mysql.java.CDCUtils.sameCreateMysql;

@Slf4j
public class mysqlCDC {

    /**
        在mysqlCDC中的mySqlCDC 注意com.alibaba.ververica与com.ververica不可以同时使用，会出现冲突
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
        Connection mysqlConnection=null;
        DatabaseMeta targetDbmeta = null; //
        try {

            if (originalDatabaseType.equals("MYSQL")) { //kettle中mysql需要引入jar包，然而会和flink-connector-mysql-cdc冲突，因此重写连接和建表代码
                String url = "jdbc:mysql://" + originalIp + ":" + originalPort + "/" + originalDbname+"?characterEncoding=UTF-8&serverTimezone=GMT%2B8&useSSL=false";
                Properties props = new Properties();
                props.setProperty("user", originalUsername);
                props.setProperty("password", originalPassword);
                mysqlConnection= DriverManager.getConnection(url, props);
            }
            targetDbmeta = new DatabaseMeta(targetDbname, targetDatabaseType, "Native(JDBC)", targetIp, targetDbname, targetPort, targetUsername, targetPassword);
        } catch (Exception e) {
            kettleLog.logError(e + "");
        }
        kettleLog.logBasic("源数据库、目标数据库连接成功！");
        if (mysqlConnection != null && targetDbmeta != null) {

            Database targetDatabase = new Database(targetDbmeta);
            targetDatabase.connect(); //连接数据库

            try {
                if (tableList != null) {//填入表名的
                    List<String> allTableList = null;
                    if (tableList.contains(",")) {
                        allTableList = Arrays.asList(tableList.split(","));
                    } else {
                        allTableList = Collections.singletonList(tableList);
                    }


                    if (allTableList.size() > 0) {
                        for (String table : allTableList) {
                            String sql1 =null;

                            DatabaseMetaData metaData = mysqlConnection.getMetaData();
                            ResultSet rs = metaData.getColumns(null, originalSchema,  table, null);

                            StringBuilder createTableStatement = new StringBuilder("CREATE TABLE " +targetSchema + "." + table+" (");
                            Map<String,Object> cloumnMap=new HashMap<>();

                            while (rs.next()) {
                                String columnName = rs.getString("COLUMN_NAME");
                                int dataType = rs.getInt("DATA_TYPE");
                                int columnSize = rs.getInt("COLUMN_SIZE");
                                boolean isNullable = rs.getBoolean("NULLABLE");
                                cloumnMap.put(columnName,dataType);

                                String columnType = getColumnType(dataType, columnSize);
                                createTableStatement.append(columnName).append(" ").append(columnType);
                                if (!isNullable) {
                                    createTableStatement.append(" NOT NULL");
                                }
                                createTableStatement.append(", ");
                            }

                            createTableStatement.setLength(createTableStatement.length() - 2); // Remove the last comma and space
                            createTableStatement.append(")");

                            sql1=createTableStatement.toString();

                            if (etlTime.length() > 0) {
                                sql1=sql1. substring(0, sql1.length() - 1);
                                sql1 = sql1 + ",";
                                sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
                                sql1 = sql1 + Const.CR + ");";

                            }
                            if (sql1.length() > 0) {
                                if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                                    //建索引、创建表
                                    sameCreateMysql(table, sql1, cloumnMap, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName); //创建表
                                    kettleLog.logBasic(table + " 创建输出表成功！");
                                }
                            }


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
                                    .with("database.port", Integer.valueOf(originalPort))
                                    .with("database.user", originalUsername)
                                    .with("database.password", originalPassword)
                                    .with("database.dbname", originalDbname)
                                    .with("database.server.name", "my-mysql-server1")
                                    .with("table.include.list", originalSchema + "." + table)
                                    .with("include.schema.changes", "false")
                                    .with("name", "my-connector-mysql-1")
                                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                                    .with("offset.storage.file.filename", "D:\\\\Debezium\\\\offset\\\\mysql\\\\file.dat")
                                    .with("offset.flush.interval.ms", 5000)
                                    .with("database.history", FileDatabaseHistory.class.getName())
                                    .with("database.history.file.filename", "D:\\\\Debezium\\\\offset\\\\mysql\\\\dbhistory.dat")
                                    .with("database.history.kafka.bootstrap.servers", kafkaipport)
                                    .with("database.history.kafka.topic", topic)
                                    .with("logger.level", "DEBUG")

                                    .build();

                            EmbeddedEngine engine = EmbeddedEngine.create()
                                    .using(config)
                                    .notifying(record -> {
                                        String key = String.valueOf(record.key());

                                        Struct structValue = (Struct) record.value();

                                        commonCrud(structValue, table,key, topic,  props, targetSchema, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime,index);

                                    })
                                    .build();

                            // 启动 engine
                            engine.run();

                        }

                    }
                }
            } finally {

                targetDatabase.disconnect();
            }
        }
    }

    private static String getColumnType(int dataType, int columnSize) {
        if (dataType == Types.VARCHAR || dataType == Types.CHAR) {
            return "VARCHAR(" + columnSize + ")";
        } else if (dataType == Types.INTEGER) {
            return "INT";
        } else if (dataType == Types.DATE) {
            return "DATE";
        } else {
            return "VARCHAR(255)"; // Default to VARCHAR if data type is unknown
        }
    }

}
