import org.junit.Test

class ScalaTester {

  @Test
  def min(): Unit ={
    val l = List("2017-01-01", "2017-01-02", "2017-00-02")
    println(l.min)
  }

}
