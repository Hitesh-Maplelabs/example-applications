/**
  * Name:       WordCountStreamApp
  * Purpose:    Application entry point to create, configure and start flink streaming job.
  * Author:     PNDA team
  *
  * Created:    20/04/2018
  */

/*
Copyright (c) 2018 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.
Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.pnda.examples.flink;

import java.io.FileWriter;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.flink.metrics.Counter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.Accumulator;

import com.cisco.pnda.examples.flink.util.WordCountData;
import com.cisco.pnda.examples.flink.*;


final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
    public static Counter counter_x;
    public static Counter counter_y;
	static int which_count = 0;
	private IntCounter accumulator_count= new IntCounter();

    @Override
    public void open(Configuration config) {
        counter_x = getRuntimeContext().getMetricGroup().counter("sample-counter1");
		counter_y = getRuntimeContext().getMetricGroup().counter("sample-counter2");
		getRuntimeContext().addAccumulator("sample-accumulator", this.accumulator_count);
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] tokens = value.toLowerCase().split("\\W+");
        for (String token : tokens) {

            // This is to increase application execution time with small input data.
            try{
                // FileWriter wr = new FileWriter("123.txt");
                // wr.write(String.valueOf("test"));
                // wr.close();
                TimeUnit.SECONDS.sleep(1);
            }
            catch(Exception e){
                System.out.println("Exception : " + e);
            }

            if (token.length() > 0) {
                if (token.equals("apache")) {
                // if (1 == 1) {
                    this.counter_x.inc();
                    this.accumulator_count.add(1);
                    System.out.println("new testing ");
                    doSend("my_metric", this.accumulator_count);
                }
                else if (token.equals("Licenses")) {
                    System.out.println("new testing2 ");
                    this.counter_y.inc();
                    this.accumulator_count.add(1);
                    doSend("my_metric", this.accumulator_count);
                }
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }

    public static IntCounter doSend(String MetricName, IntCounter MetricValue){
        Properties props = AppConfig.loadProperties();
        String metricsUrl = props.getProperty("environment.metric_logger_url");
        // String metricsUrl = "http://10.0.1.1";
        String appName = props.getProperty("component.application");
        Logger logger;

        logger = Logger.getLogger(WordCountBatchApp.class.getName());
        String Counter = MetricValue.toString();
        String[] CounterValue = Counter.split(" ");
        System.out.println(CounterValue[1]);
        System.out.println(metricsUrl);

        try{
            FileWriter wr = new FileWriter("123.txt");
            wr.write(String.valueOf(metricsUrl));
            wr.write(String.valueOf(appName));
            wr.write(String.valueOf(Counter));
            wr.close();
            // TimeUnit.SECONDS.sleep(1);
        }
        catch(Exception e){
            System.out.println("Exception : " + e);
        }

        try {
            DefaultHttpClient httpClient = new DefaultHttpClient();
            HttpPost post = new HttpPost(metricsUrl);
            // post.setHeader("Content-type", "application/json");
            long ts = Calendar.getInstance().getTimeInMillis();
            String body;

            body = String.format("\"{" +
                            "\\\"data\\\": [ \\\"source\\\":\\\"application.%s\\\", " +
                            "\\\"metric\\\":\\\"application.kpi.%s.%s\\\"," +
                            "\\\"value\\\":\\\"%s\\\",\\\"timestamp\\\":%d]," +
                            "\\\"timestamp\\\": %d" + "}\"",
                    appName, appName, MetricName, MetricValue, ts, ts);

            logger.info(body);
            post.setEntity(new StringEntity(body));
            HttpResponse response = httpClient.execute(post);

            if (response.getStatusLine().getStatusCode() != 200){
                logger.severe("POST failed: " + metricsUrl + " response:" + response.getStatusLine().getStatusCode());
            }
        }
        catch (Exception e){
            logger.severe("POST failed: " + metricsUrl);
            e.printStackTrace();
        }
        return MetricValue;
    }
}

@SuppressWarnings("serial")
public class WordCountBatchApp{

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setLatencyTrackingInterval(5L);
        Tokenizer token_obj = new Tokenizer();

        // get input data
        DataSet<String> text;

        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        }
        else {
            // get default test text data
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
            /* text = env.fromElements("\"Apache Apache Apache Apache Apache\",\n" +
                    "                \"To be, or not to be,--that is the question:--\",\n" +
                    "                \"Whether 'tis nobler in the mind to suffer\",\n" +
                    "                \"The slings and arrows of outrageous fortune\",\n" +
                    "                \"Or to take arms against a sea of troubles,\",\n" +
                    "                \"And by opposing end them?--To die,--to sleep,--\",\n" +
                    "                \"No more; and by a sleep to say we end\",\n" +
                    "                \"The heartache, and the thousand natural shocks\",\n" +
                    "                \"That flesh is heir to,--'tis a consummation\",\n" +
                    "                \"Devoutly to be wish'd. To die,--to sleep;--\",\n" +
                    "                \"To sleep! perchance to dream:--ay, there's the rub;\",\n" +
                    "                \"For in that sleep of death what dreams may come,\",\n" +
                    "                \"When we have shuffled off this mortal coil,\",\n" +
                    "                \"Must give us pause: there's the respect\",\n" +
                    "                \"That makes calamity of so long life;\",\n" +
                    "                \"For who would bear the whips and scorns of time,\",\n" +
                    "                \"The oppressor's wrong, the proud man's contumely,\",\n" +
                    "                \"The pangs of despis'd love, the law's delay,\",\n" +
                    "                \"The insolence of office, and the spurns\",");*/
            System.out.println("checking");
        }

        DataSet<Tuple2<String, Integer>> counts =
            // split up the lines in pairs (2-tuples) containing: (word,1)
            text.flatMap(token_obj)
            // group by the tuple field "0" and sum up tuple field "1"
            .groupBy(0)
            .sum(1);

        // emit result
        if (params.has("output")) {
                counts.writeAsCsv(params.get("output"), "\n", " ");
                // execute program
                env.execute("WordCount Example");
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        JobExecutionResult result;
        Integer life_wordcount, after_wordcount;

        // result = env.getLastJobExecutionResult().getAccumulatorResult("which-count");
        life_wordcount = env.getLastJobExecutionResult().getIntCounterResult("life-count");
        System.out.println("Accumulator Result for LIFE Word Count : " + life_wordcount);
    }
}
