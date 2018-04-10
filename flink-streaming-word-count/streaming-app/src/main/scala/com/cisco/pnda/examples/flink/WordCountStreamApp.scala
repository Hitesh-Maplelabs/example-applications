package com.cisco.pnda.examples.flink;


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Flink streaming windowed example of "WordCount" program.
 * 
 * This application connects to a server's socket and reads input data from the socket.
 * {{{
 * nc -l 9100
 * }}}
 * and run this example with the hostname, port and windowtime as arguments, by default it will run on edge node
 */


object WordCountStreamApp {

      /** Main program method */
  def main(args: Array[String]) : Unit = {

    // Host and port to connect
    var hostname: String = "localhost"
    var port: Int = 0
    var windowtime: Int = 0

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      windowtime = if (params.has("windowtime")) params.getInt("windowtime") else 5
      port = params.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }
    
    //  streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
    // input data by connecting to the socket
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

    // parse the data, group it, window it, and aggregate the counts 
    val windowCounts = text
          .flatMap { w => w.split("\\s") }
          .map { w => WordCount(w, 1) }
          .keyBy("word")
          .timeWindow(Time.seconds(windowtime))
          .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordCount(word: String, count: Long)
}

