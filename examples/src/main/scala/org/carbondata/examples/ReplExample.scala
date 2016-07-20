package org.carbondata.examples

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.examples.util.InitForExamples

import scala.tools.nsc.transform.patmat.Logic.PropositionalLogic.True


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
