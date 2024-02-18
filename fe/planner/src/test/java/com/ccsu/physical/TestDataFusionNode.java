package com.ccsu.physical;

import arrow.datafusion.protobuf.FileScanExecConf;
import arrow.datafusion.protobuf.JoinFilter;
import arrow.datafusion.protobuf.JoinType;
import arrow.datafusion.protobuf.NestedLoopJoinExecNode;
import arrow.datafusion.protobuf.ParquetScanExecNode;
import arrow.datafusion.protobuf.PhysicalBinaryExprNode;
import arrow.datafusion.protobuf.PhysicalColumn;
import arrow.datafusion.protobuf.PhysicalExprNode;
import arrow.datafusion.protobuf.PhysicalPlanNode;
import arrow.datafusion.protobuf.ScanLimit;
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