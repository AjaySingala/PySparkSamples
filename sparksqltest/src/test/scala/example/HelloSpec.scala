package example
// run with "test" or ~testQuick on sbt prompt.
// sbt:sparksqltest> Test / testOnly example.HelloSpec


//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
import org.scalatest._

class HelloSpec extends FlatSpec with Matchers {
  "The Hello object" should "say hello" in {
    Hello.greeting shouldEqual "hello"
  }
}
