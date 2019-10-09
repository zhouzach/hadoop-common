package config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

object FileConfig {

  private val fileConf = ConfigFactory.parseFile(new File("./application.conf"))

  val default = ConfigFactory.load() //default environment

  val combinedConfig: Config = fileConf.withFallback(default)

  val oracleConfig: Config = combinedConfig.getConfig("oracle")

  val emailConfig: Config = combinedConfig.getConfig("email")


  printConf(emailConfig)
  def printConf(config: Config): Unit = println(config.root()
    .render(ConfigRenderOptions.concise().setFormatted(true).setJson(true)))

}
