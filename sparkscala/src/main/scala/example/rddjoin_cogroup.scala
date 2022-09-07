// rddjoin_cogroup.scala
// spark-submit ./sparkscala_2.11-0.1.0-SNAPSHOT.jar --class sparkbasics.JoinCogroup

package example

// Basic ways to join the data:
// -join (inner join)
// -rightOuterJoin
// -leftOuterJoin
// -fullOuterJoin
// -semiJoin
// cogroup is also known as groupWith.

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object JoinCogroup extends App {
    case class Row(id: Int, value: String)

    private[this] implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    // join using DF.
    println("join()...")
    val schema = Seq("id", "value")
    val r1_data = Seq((1, "A1"), (2, "A2"), (3, "A3"), (4, "A4"))
    val r2_data = Seq((3, "A3"), (4, "A4"), (4, "A4_1"), (5, "A5"), (6, "A6"))
    val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", 
        "left_semi", "left_anti")

    val r1 = r1_data.toDF(schema:_*)
    val r2 = r2_data.toDF(schema:_*)

    // joinTypes.foreach ({ joinType => println(s"${joinType.toUpperCase()} JOIN"); r1.join(right = r2, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show() })
    joinTypes.foreach ({ joinType => 
        println(s"${joinType.toUpperCase()} JOIN"); 
        r1.join(right = r2, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show() 
    })

    // // Cogroup.
    // println("\ncogroup()...")
    // val myrdd1 = sc.parallelize(List((1,"physics"),(2,"sanskrit"),(3,"hindi"),(4,"physical"),(6,"computer")))
    // val myrdd2 = sc.parallelize(List((4,"english"),(5,"arts"),(6,"social"),(1,"bio"),(1,"chemistry")))
    // val result = myrdd1.cogroup(myrdd2)
    // result.collect().foreach(println)

    // // join and groupBy using RDD.
    // println("join() and groupBy() using RDD...")
    // case class Item(id:String, name:String, unit:Int, companyId:String)
    // case class Company(companyId:String, name:String, city:String)

    // val i1 = Item("1", "first", 2, "c1")
    // val i2 = i1.copy(id="2", name="second")
    // val i3 = i1.copy(id="3", name="third", companyId="c2")
    // val items = sc.parallelize(List(i1,i2,i3))

    // val c1 = Company("c1", "company-1", "city-1")
    // val c2 = Company("c2", "company-2", "city-2")
    // val companies = sc.parallelize(List(c1,c2))

    // val groupedItems = items.groupBy( x => x.companyId) 
    // val groupedComp = companies.groupBy(x => x.companyId)

    // groupedItems.join(groupedComp).take(10).foreach(println)
}
