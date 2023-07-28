package com.mysql.java.test;

import com.mysql.java.mysqlCDC;



public class mysqlTest {
    public static void main(String[] args) throws Exception {
        mysqlCDC.incrementData("MYSQL", "test", "test", "127.0.0.1", "3306", "root",
                "123456", "POSTGRESQL", "postgres", "test", "127.0.0.1", "5432", "postgres",
                "123456", "fangtest1","10.0.108.51:9092","mysql_cdc","a","index_a","etltime");
    }
}
