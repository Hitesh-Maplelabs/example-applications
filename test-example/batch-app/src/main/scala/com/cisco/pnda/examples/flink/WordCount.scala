package flink

// package flink-test

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

import scala.concurrent.{ExecutionContext => ConcurrentExecutionContext}

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {

  val props = AppConfig.loadProperties
  val metricsUrl = props.getProperty("environment.metric_logger_url")
  val appName = props.getProperty("component.application")

  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    val words: DataSet[String] = text.flatMap { line =>
      val ret = line.toLowerCase.split("\\W+")
      ret
    }

    val wordsPairs: DataSet[(String, Int)] = words.map { (_, 1) }
    // wordsPairs.print()
    val groupedCountPairs: GroupedDataSet[(String, Int)] = wordsPairs.groupBy(0)
    val wordCounts: AggregateDataSet[(String, Int)]  = groupedCountPairs.sum(1)
    // wordCounts.print()
    //val word7Groups: GroupedDataSet[(String, Int)] = wordsPairs.groupBy {_ match { case (s, i) =>
     // s.length % 1 }}
    val wordGroups: DataSet[Int] =  wordsPairs.map(x => x._2)
    val total_words = wordGroups.reduce(_+_)
    // total_words.map(x => Asa("no", x.toString)).print("name")
    total_words.map(x => doSend("Total_words", x.toString)).print("name")
    env.execute("execute job")

  }

  def Asa(clas:String, name: String) ={
    println(name)
  }

  def doSend(metricName: String, metricValue: String) = {
    // try {
      val httpClient = new DefaultHttpClient()
      val post = new HttpPost(metricsUrl)
      post.setHeader("Content-type", "application/json")
      val ts = java.lang.System.currentTimeMillis()
      val body = f"""{
                    |    "data": [{
                    |        "source": "application.$appName",
                    |        "metric": "application.kpi.$appName.$metricName",
                    |        "value": "$metricValue",
                    |        "timestamp": $ts%d
                    |    }],
                    |    "timestamp": $ts%d
                    |}""".stripMargin

      // logger.debug(body)
      post.setEntity(new StringEntity(body))
      val response = httpClient.execute(post)
      /*if (response.getStatusLine.getStatusCode() != 200) {
        logger.error("POST failed: " + metricsUrl + " response:" + response.getStatusLine.getStatusCode())
      }

    } catch {
      case NonFatal(t) => {
        logger.error("POST failed: " + metricsUrl)
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
      }
    }
  }*/

  }
}
