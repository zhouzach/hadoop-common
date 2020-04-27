package spark.writeTo

import java.util.Properties

object DbConfig {

  val user: String = FileConfig.dataSource.getString("user")
  val password: String = FileConfig.dataSource.getString("password")
  val url: String = FileConfig.dataSource.getString("url")
  val driverClass: String = FileConfig.dataSource.getString("driverClass")

  val pop: Properties = new Properties()
  pop.setProperty("user", user)
  pop.setProperty("password", password)
  pop.setProperty("driver", driverClass)



}
