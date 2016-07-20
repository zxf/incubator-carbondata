/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbondata.examples

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.examples.util.InitForExamples

// scalastyle:off println

object ReplExample {
  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("ReplExample")

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")

    while(true) {
      val input = readLine("sql> ")

      if (input == "exit") {
        sys.exit()
      } else {
        try {
          cc.sql(input).show()
        } catch {
          case e => println(e)
        }
      }
    }

  }
}

// scalastyle:on println

