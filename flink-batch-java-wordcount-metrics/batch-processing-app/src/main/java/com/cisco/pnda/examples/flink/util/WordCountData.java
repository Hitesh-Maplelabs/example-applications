/*
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

package com.cisco.pnda.examples.flink.util;
import java.util.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Provides the default data sets used for the WordCount example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class WordCountData {
        public static final int REP_COUNT = 1;
        // public static final int REP_COUNT = 1073741824;
        public static final String[] WORDS = new String[] {
                "apache apache Apache Apache Apache Apache Apache",
                "apache apache To be, apache or not to be,--that is the question:--"
        };

        public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
            // return env.fromElements(WORDS);
            List<String> list = new ArrayList<String>();
            for(int i=1;i<=REP_COUNT;i++) {
                for(int j=1;j<=REP_COUNT;j++) {
                    list.addAll(Arrays.asList(WORDS));
                }
            }

            String[] bigWord = (String[]) list.toArray(new String[0]);
             return env.fromElements(bigWord);
        }
}