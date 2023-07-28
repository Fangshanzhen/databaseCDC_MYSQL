package com.mysql.java.test;


import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;

public class test {

    public static void main(String[] args) {
        String filePath = "C:\\Users\\fsz\\Desktop\\test.xlsx";
        int columnIndex = 1; // 要筛选的列索引，从0开始

        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0); // 获取第一个工作表

            for (Row row : sheet) {
                Cell cell = row.getCell(columnIndex);
                if (cell != null  && cell.getNumericCellValue() != 0) {
                    int value = (int) cell.getNumericCellValue();
                    if (value != 0) {
                        System.out.println(value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
