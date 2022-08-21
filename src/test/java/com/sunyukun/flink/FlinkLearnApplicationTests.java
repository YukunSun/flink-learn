package com.sunyukun.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

//@SpringBootTest
class FlinkLearnApplicationTests {

    /**
     * 批处理：统计每个单词出现的频次
     */
    @Test
    void batchWordCount() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDataSource = environment.fromCollection(Lists.newArrayList("hello test", "hello world", "hi world"));
//        DataSource<String> lineDataSource = environment.readTextFile("/Users/didi/Documents/ideaworks/flink-learn/src/test/resources/words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> tuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //按照 word 进行分组
        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = tuple.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        sum.print();
    }

    @Test
    void streamWordCount() {

    }
}
