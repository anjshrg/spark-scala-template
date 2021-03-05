package com.ecom.traits

import scopt.OptionParser

case class AppConfig(
                      executionMode: String = "incremental",
                      loadDate: String = "",
                      startDate: String = "",
                      endDate: String = ""
                    )

class AppOptionParser extends OptionParser[AppConfig]("Ecom report config") {
  head("clickstream-analytics", "1.x")
  opt[String]('m', "execution-mode").required().action(
    (value, arg) => {
      arg.copy(executionMode = value)
    }
  ).text("Execution mode -> can be incremental or historical")

  opt[String]('d', "load-date").optional().action((value, arg) => arg.copy(loadDate = value))
  // TODO - Complete for other arguments
}
