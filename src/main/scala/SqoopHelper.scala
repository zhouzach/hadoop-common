object SqoopHelper {

  /**
    * https://community.hortonworks.com/questions/20012/how-to-use-merge-in-scoop-import.html
    */
  def incremental(): Unit = {

    """
      |
      |sqoop import --connect jdbc:mysql://localhost/test --table emp \
      |
      |--username hive -password hive --incremental lastmodified --merge-key empid --check-column cr_date \
      |
      |--target-dir /sqoop/empdata/
      |
    """.stripMargin
  }

}
