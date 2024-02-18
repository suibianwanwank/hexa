package com.ccsu.physical;

import arrow.datafusion.FileScanExecConf;
import arrow.datafusion.JoinFilter;
import arrow.datafusion.JoinType;
import arrow.datafusion.NestedLoopJoinExecNode;
import arrow.datafusion.ParquetScanExecNode;
import arrow.datafusion.PhysicalBinaryExprNode;
import arrow.datafusion.PhysicalColumn;
import arrow.datafusion.PhysicalExprNode;
import arrow.datafusion.PhysicalPlanNode;
import arrow.datafusion.ScanLimit;
import org.junit.Test;

public class TestDataFusionNode {

    @Test
    public void test1() {
        ParquetScanExecNode left = ParquetScanExecNode.newBuilder()
                .setBaseConf(FileScanExecConf.newBuilder().setLimit(ScanLimit.newBuilder().setLimit(1).build()).build())
                .build();
        ParquetScanExecNode right = ParquetScanExecNode.newBuilder()
                .setBaseConf(FileScanExecConf.newBuilder().setLimit(ScanLimit.newBuilder().setLimit(1).build()).build())
                .build();


        PhysicalBinaryExprNode binary = PhysicalBinaryExprNode.newBuilder()
                .setL(PhysicalExprNode.newBuilder()
                        .setColumn(PhysicalColumn.newBuilder().setIndex(7).build())
                        .build())
                .setR(PhysicalExprNode.newBuilder()
                        .setColumn(PhysicalColumn.newBuilder().setIndex(0).build())
                        .build())
                .setOp("AND")
                .build();
        NestedLoopJoinExecNode execNode = NestedLoopJoinExecNode.newBuilder()
                .setLeft(PhysicalPlanNode.newBuilder().setParquetScan(left).build())
                .setRight(PhysicalPlanNode.newBuilder().setParquetScan(right).build())
                .setJoinType(JoinType.FULL)
                .setFilter(JoinFilter.newBuilder().setExpression(PhysicalExprNode.newBuilder().setBinaryExpr(binary).build()).build())
                .build();
        PhysicalPlanNode root = PhysicalPlanNode.newBuilder()
                .setNestedLoopJoin(execNode)
                .build();
        System.out.println(root);
    }
}