package com.mysql.java;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Field;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelFactory;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CDCUtils {

    private static final LogChannelFactory logChannelFactory = new org.pentaho.di.core.logging.LogChannelFactory();
    private static final LogChannel kettleLog = logChannelFactory.create("数据CDC增量工具类");

    public static void deleteData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname) throws Exception {
        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        assert conn != null;
        conn.setAutoCommit(false);
        List<String> values = new ArrayList<>();
        StringBuilder sql = new StringBuilder("DELETE FROM " + targetSchema + "." + table + " WHERE "); //where x=? and y=?
        if (data.size() > 0) {
            for (String key : data.keySet()) {
                if ((String) data.get(key) == null) {
                    sql.append(key).append(" is ? ");//把@去掉   where 后面是and  where x is null and y is null
                } else {
                    sql.append(key).append("=? ");
                }
                sql.append("and ");
                values.add((String) data.get(key));  // ?中的赋值

            }
            String newsql = sql.toString().trim();

            if (newsql.endsWith("and")) {
                newsql = newsql.substring(0, newsql.length() - 3);
            }
            PreparedStatement stmt = conn.prepareStatement(newsql);
            int i = 1;
            for (Object valueObj : values) {
                stmt.setObject(i, valueObj);
                i++;
            }

            stmt.execute();
            conn.commit();
            conn.close();
            kettleLog.logBasic("有数据删除！");
        }
    }

    public static void updateData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname, String etlTime) throws Exception {
        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        assert conn != null;
        conn.setAutoCommit(false);
        List<String> values = new ArrayList<>();
        StringBuilder sql = new StringBuilder("UPDATE " + targetSchema + "." + table + " SET ");  //set x=?, y=?
        if (data.size() > 0) {
            for (String key : data.keySet()) {
                if (!key.endsWith("@")) {
                    sql.append(key).append(" =?,");
                    values.add((String) data.get(key));
                }
            }

            if (etlTime != null) {
                sql.append(etlTime + "   = NOW() ");
            }

            sql.deleteCharAt(sql.length() - 1);  //去掉最后的,
            sql.append("  where  ");
            for (String key : data.keySet()) {
                if (key.endsWith("@")) {
                    String a = key.substring(0, key.length() - 1);
                    if ((String) data.get(key) == null) {
                        sql.append(a).append(" is ? ");//把@去掉   where 后面是and  where x is null and y is null
                    } else {
                        sql.append(a).append(" =? ");//把@去掉   where 后面是and  where x=? and y=?
                    }

                    sql.append("and ");
                    values.add((String) data.get(key));
                } else {
                    continue;
                }
            }
            String newsql = sql.toString().trim();
            if (newsql.endsWith("and")) {
                newsql = newsql.substring(0, newsql.length() - 3);
            }

            PreparedStatement stmt = conn.prepareStatement(newsql);
            int i = 1;
            for (Object valueObj : values) {
                stmt.setObject(i, valueObj);
                i++;
            }

            stmt.execute();
            conn.commit();
            conn.close();
            kettleLog.logBasic("有数据更新！");
        }
    }

    public static void insertData(String targetSchema, String table, JSONObject data, String targetDatabaseType, String targetIp, String targetPort,
                                  String targetUsername, String targetPassword, String targetDbname, String index) throws Exception {

        Connection conn = getConnection(targetDatabaseType, targetIp, targetPort, targetSchema, targetUsername, targetPassword, targetDbname);
        conn.setAutoCommit(false);
        StringBuilder sql = new StringBuilder("INSERT INTO " + targetSchema + "." + table + " (");
        StringBuilder value = new StringBuilder(" VALUES (");
        if (data.size() > 0) {
            for (String key : data.keySet()) {
                sql.append(key).append(",");
                value.append("?,");
            }
            sql.deleteCharAt(sql.length() - 1).append(")");
            value.deleteCharAt(value.length() - 1).append(")");
            //加一段代码，插入数据前进行判断，防止因为有索引出现插入数据错误的情况
            // 检查是否存在冲突的索引值
            int count = 0;
            StringBuilder checkSql = new StringBuilder("SELECT COUNT(*) FROM " + targetSchema + "." + table + "  where  ");
            String[] indexList = null;
            if (index != null && index.contains(",")) {
                indexList = index.split(",");
            } else if (index != null && !index.contains(",")) {
                indexList = new String[]{index};
            }
            if (indexList != null) {
                for (String index1 : indexList) {
                    checkSql.append(index1).append(" =? ");
                    checkSql.append("and ");
                }

                String newsql = checkSql.toString().trim();
                if (newsql.endsWith("and")) {
                    newsql = newsql.substring(0, newsql.length() - 3);
                }
                PreparedStatement checkStmt = conn.prepareStatement(newsql);

                int i = 1;
                for (String index1 : indexList) {
                    if (data.keySet().contains(index1)) {
                        checkStmt.setObject(i, data.get(index1));
                    }
                    i++;
                }
                ResultSet resultSet = checkStmt.executeQuery();
                resultSet.next();
                count = resultSet.getInt(1);
                checkStmt.close();
                resultSet.close();
            }


            if (count == 0) {
                PreparedStatement stmt = conn.prepareStatement(sql.toString() + "  " + value.toString());
                int i = 1;
                for (Object valueObj : data.values()) {
                    stmt.setObject(i, valueObj);
                    i++;
                }
                stmt.execute();
                conn.commit();
            }


            conn.close();
            kettleLog.logBasic("有数据新增！");
        }
    }


    /**
     * 获取数据库连接对象
     */
    public static Connection getConnection(String targetDatabaseType, String targetIp, String targetPort, String targetSchema,
                                           String targetUsername, String targetPassword, String targetDbname) throws SQLException {

        if (targetDatabaseType.equals("MYSQL")) {
            String url = "jdbc:mysql://" + targetIp + ":" + targetPort + "/" + targetDbname;
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }

        if (targetDatabaseType.equals("POSTGRESQL")) {
            String url = "jdbc:postgresql://" + targetIp + ":" + targetPort + "/" + targetDbname + "?searchpath=" + targetSchema;
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }

        if (targetDatabaseType.equals("ORACLE")) {
            String url = "jdbc:oracle:thin:@" + targetIp + ":" + targetPort + ":" + targetDbname;
            Properties props = new Properties();
            props.setProperty("user", targetUsername);
            props.setProperty("password", targetPassword);
            return DriverManager.getConnection(url, props);
        }
        return null;

    }


    public static boolean checkTableExist(Database database, String schema, String tablename) throws Exception {
        try {

            // Just try to read from the table.
            String sql = "select 1 from  " + schema + "." + tablename;
            Connection connection = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                connection = database.getConnection();
                stmt = connection.createStatement();
                stmt.setFetchSize(1000);
                rs = stmt.executeQuery(sql);
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                close(connection, stmt, rs);
            }
        } catch (Exception e) {
            throw new KettleDatabaseException("", e);
        }

    }


    public static void close(Connection connection, Statement statement, ResultSet resultSet) throws SQLException {

        if (resultSet != null) {
            resultSet.close();
        }

        if (statement != null) {
            statement.close();
        }
    }

    public static void sameCreate(String table, String sql1, RowMetaInterface rowMetaInterface, DatabaseMeta originalDbmeta, String originalDatabaseType, String targetDatabaseType, String targetSchema, Database targetDatabase, String index, String indexName) throws KettleDatabaseException {
        /**
         * 新建索引语句，对于text、longtext类型的字段建索引需要指定长度，否则会报错
         */
        if (sql1.toLowerCase().contains("create")) {
            if (indexName != null && indexName.length() > 0 && index.length() > 0) {
                for (int i = 0; i < rowMetaInterface.size(); i++) {
                    ValueMetaInterface v = rowMetaInterface.getValueMeta(i);
                    String x = originalDbmeta.getFieldDefinition(v, null, null, false); // ipid LONGTEXT  b TEXT

                    if (index.contains(",")) {   //ipid,pid 复合索引
                        String[] indexes = index.split(",");
                        for (int j = 0; j < indexes.length; j++) {
                            String in = indexes[j];
                            String x1 = null;
                            x1 = transform(x, x1, in);
                            if (x1 != null && x1.length() > 0) {
                                if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                    x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                                }
                                if (sql1.contains(x)) {
                                    sql1 = sql1.replace(x, x1);
                                }
                            }
                        }
                    } else {   //只有一个索引字段
                        String x1 = null;
                        x1 = transform(x, x1, index);

                        if (x1 != null && x1.length() > 0) {
                            if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                            }
                            if (sql1.contains(x)) {
                                sql1 = sql1.replace(x, x1);   //将text类型修改为varchar(1000)
                            }
                        }
                    }
                }
                if (targetSchema.length() > 0) {   //分情况添加索引语句
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + "." + table + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                } else {
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                }
            }
            if (targetDatabaseType.equals("POSTGRESQL")) { //postgresql 没有主键的时候更新、删除会出现报错
                String alterSql = " ALTER TABLE " + targetSchema + "." + table + " REPLICA IDENTITY FULL;";
                sql1 = sql1 + Const.CR + alterSql;
            }
        }


        if (sql1.contains(";")) {
            String[] sql2 = sql1.split(";");
            for (String e : sql2) {
                if (!e.trim().equals(""))
                    targetDatabase.execStatement(e.toLowerCase());  //创建表和索引加时间戳
            }
        }
    }

    public static String transform(String x, String x1, String in) {
        if (x.contains(in)) {  //ipid
            if (x.contains("TEXT") && !x.contains("LONG")) {
                x1 = x.replace("TEXT", "VARCHAR(255)");
            } else if (x.contains("LONGTEXT")) {
                x1 = x.replace("LONGTEXT", "VARCHAR(255)");
            } else if (x.contains("text") && !x.contains("long")) {
                x1 = x.replace("text", "VARCHAR(255)");
            } else if (x.contains("longtext")) {
                x1 = x.replace("longtext", "VARCHAR(255)");
            }
        }

        return x1;
    }

    public static void createTable(String sql, String originalDatabaseType, DatabaseMeta originalDbmeta, String originalSchema, Database originalDatabase, String table, Database targetDatabase, String targetSchema, String targetDatabaseType, String etlTime, String index, String indexName) throws Exception {

        if (originalDatabaseType.equals("ORACLE")) {
            sql = "select * from " + originalSchema + "." + table + " where rownum <=10 ";  //用sql来获取字段名及属性以便在目标库中创建表
        } else if (originalDatabaseType.equals("MSSQL")) {
            sql = "select top 10 * from " + originalDatabase + "." + originalSchema + "." + table;   //sqlserver  没有limit 用top
        } else {
            sql = "select * from " + originalSchema + "." + table + "  limit 10;";
        }
        RowMetaInterface rowMetaInterface = originalDatabase.getQueryFieldsFromPreparedStatement(sql);

        String sql1 = targetDatabase.getDDLCreationTable(targetSchema + "." + table, rowMetaInterface);
        if (etlTime.length() > 0) {
            int a = sql1.lastIndexOf(")"); //最后一个)
            if (a > 0) {
                sql1 = sql1.replace(sql1.substring(a), "");
            }
            sql1 = sql1 + ",";
            sql1 = sql1 + etlTime + "  " + "TIMESTAMP " + " NOT NULL DEFAULT CURRENT_TIMESTAMP";
            sql1 = sql1 + Const.CR + ");";

        }
        if (sql1.length() > 0) {
            if (!checkTableExist(targetDatabase, targetSchema, table)) {  //判断目标数据库中表是否存在
                //建索引、创建表
                sameCreate(table, sql1, rowMetaInterface, originalDbmeta, originalDatabaseType, targetDatabaseType, targetSchema, targetDatabase, index, indexName); //创建表
                kettleLog.logBasic(table + " 创建输出表成功！");
            }
        }
    }


    public static void commonCrud(org.apache.kafka.connect.data.Struct structValue, String table, String key, String topic, Properties props, String targetSchema, String targetDatabaseType, String targetIp, String targetPort, String targetUsername, String targetPassword,
                                  String targetDbname, String etlTime, String index) {

        if ((String.valueOf(structValue).contains("after") || String.valueOf(structValue).contains("before")) && String.valueOf(structValue).contains(table)) {
            org.apache.kafka.connect.data.Struct afterStruct = structValue.getStruct("after");
            org.apache.kafka.connect.data.Struct beforeStruct = structValue.getStruct("before");
            JSONObject operateJson = new JSONObject();
            JSONObject sqlJson = new JSONObject();

            //操作类型
            String operate_type = "";

            List<Field> fieldsList = null;
            List<Field> beforeStructList = null;

            if (afterStruct != null && beforeStruct != null) {
                System.out.println("这是修改数据:  " + sqlJson);
                operate_type = "update";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
                beforeStructList = beforeStruct.schema().fields();
                for (Field field : beforeStructList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    sqlJson.put(fieldName + "@", fieldValue);  //字段后面加@
                }

            } else if (afterStruct != null) {
                System.out.println("这是新增数据:  " + sqlJson);
                operate_type = "insert";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
            } else if (beforeStruct != null) {
                System.out.println("这是删除数据:  " + sqlJson);
                operate_type = "delete";
                fieldsList = beforeStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    sqlJson.put(fieldName, fieldValue);
                }
            } else {
                System.out.println("-----------数据无变化-------------");
            }

            operateJson.put("sqlJson", sqlJson);
            org.apache.kafka.connect.data.Struct source = structValue.getStruct("source");
            //操作的数据库名
            String database = source.getString("db");
            //操作的表名
            String table1 = source.getString("table");

//            //操作的时间戳（单位：毫秒）
//            Object operate_ms = source.get("ts_ms");
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            String date = sdf.format(new Date((Long) operate_ms));

            //操作的时间戳（单位：毫秒）
            Object operate_ms = source.get("ts_ms");
            Object operate_ms1 = structValue.get("ts_ms");

            Instant instantUtc = Instant.ofEpochMilli(Long.valueOf(String.valueOf(operate_ms)));

            // 减去8小时
            if (Long.valueOf(String.valueOf(operate_ms)) > (Long.valueOf(String.valueOf(operate_ms1)))) {
                instantUtc = instantUtc.minus(8, ChronoUnit.HOURS);
            }

            // 转换为目标时区的 LocalDateTime，这里以东八区为例
            ZoneId zoneId = ZoneId.of("Asia/Shanghai"); // 东八区
            LocalDateTime datetimeLocal = LocalDateTime.ofInstant(instantUtc, zoneId);

            // 使用DateTimeFormatter格式化时间
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            String date = datetimeLocal.format(formatter);


            operateJson.put("database", database);
            operateJson.put("table", table1);
            operateJson.put("operate_ms", date);
            operateJson.put("operate_type", operate_type);

            if (table1.equals(table)) {

                KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, String.valueOf(operateJson));
                try {
                    RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                } finally {
                    kafkaProducer.close();
                }
                kettleLog.logBasic("数据写入kafaka成功！");

//                                            // 将解析出来的数据插入到下游数据库中
                if ("INSERT".toLowerCase().equals(operate_type)) {
                    try {
                        insertData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if ("UPDATE".toLowerCase().equals(operate_type)) {
                    try {
                        updateData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname, etlTime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if ("DELETE".toLowerCase().equals(operate_type)) {
                    try {
                        deleteData(targetSchema, table1, sqlJson, targetDatabaseType, targetIp, targetPort, targetUsername, targetPassword, targetDbname);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }



    public static void sameCreateMysql(String table, String sql1, Map map,
                                       String originalDatabaseType, String targetDatabaseType, String targetSchema,
                                       Database targetDatabase, String index, String indexName) throws KettleDatabaseException {
        /**
         * 新建索引语句，对于text、longtext类型的字段建索引需要指定长度，否则会报错
         */
        if (sql1.toLowerCase().contains("create")) {
            if (indexName != null && indexName.length() > 0 && index.length() > 0) {
                for (Object key:map.keySet()) {

                    String x = key+ "     " +map.get(key); // ipid LONGTEXT  b TEXT

                    if (index.contains(",")) {   //ipid,pid 复合索引
                        String[] indexes = index.split(",");
                        for (int j = 0; j < indexes.length; j++) {
                            String in = indexes[j];
                            String x1 = null;
                            x1 = transform(x, x1, in);
                            if (x1 != null && x1.length() > 0) {
                                if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                    x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                                }
                                if (sql1.contains(x)) {
                                    sql1 = sql1.replace(x, x1);
                                }
                            }
                        }
                    } else {   //只有一个索引字段
                        String x1 = null;
                        x1 = transform(x, x1, index);

                        if (x1 != null && x1.length() > 0) {
                            if (originalDatabaseType.equals("POSTGRESQL") && targetDatabaseType.equals("MYSQL") && x.contains("TEXT")) {
                                x = x.replace("TEXT", "LONGTEXT");  //pg里面text类型同步到mysql中会变成longtext
                            }
                            if (sql1.contains(x)) {
                                sql1 = sql1.replace(x, x1);   //将text类型修改为varchar(1000)
                            }
                        }
                    }
                }
                if (targetSchema.length() > 0) {   //分情况添加索引语句
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + "." + table + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                } else {
                    String indexSql = "CREATE UNIQUE INDEX " + indexName + " ON " + targetSchema + " (" + index + "); ";
                    sql1 = sql1 + Const.CR + indexSql;
                }
            }
            if (targetDatabaseType.equals("POSTGRESQL")) { //postgresql 没有主键的时候更新、删除会出现报错
                String alterSql = " ALTER TABLE " + targetSchema + "." + table + " REPLICA IDENTITY FULL;";
                sql1 = sql1 + Const.CR + alterSql;
            }
        }


        if (sql1.contains(";")) {
            String[] sql2 = sql1.split(";");
            for (String e : sql2) {
                if (!e.trim().equals(""))
                    targetDatabase.execStatement(e.toLowerCase());  //创建表和索引加时间戳
            }
        }
    }






}
