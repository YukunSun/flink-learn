package com.sunyukun.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    /**
     * 流式
     */
    @Test
    void streamWordCount() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = environment.fromCollection(Lists.newArrayList("hello test", "hello world", "hi world"));

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple = stringDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();

        //上面只是定义，还没确切执行
        environment.execute();
    }

    /**
     * 模拟从服务器获取数据
     * <p>
     * $ nc -lk 7777
     *
     * @throws Exception
     */
    @Test
    void simulationStream() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = environment.socketTextStream("192.168.1.5", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple = stringDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();

        //上面只是定义，还没确切执行
        environment.execute();
    }
}
