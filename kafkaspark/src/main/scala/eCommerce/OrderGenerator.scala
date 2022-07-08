// OrderGenerator.scala
package eCommerce

import java.util.Date

import eCommerce.Domain.OrderRecord
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object OrderGenerator {
    val customers: List[(Int, String)] = List((1,"John"), (2, "Mary"), (3, "Joe"), 
        (4, "Deborah"), (5, "Chris"), (6, "Caleb"), (7, "Carter"))
    val products: List[(Int, String, String, Double)] = List(
        (1, "Laptop", "Computers", 799), 
        (2, "Mobile", "Mobile Phones", 299), 
        (3, "Pens", "Stationery", 1.99), 
        (4, "Pencils", "Stationery", 1.59), 
        (5, "Call of Duty", "XBOX Games", 49.99), 
        (6, "Halo Infinite", "XBOX Games", 59.99))
    val paymentTypes = Seq("Credit Card", "Debit Card", "Bank Transfer")
    val citiesCountries: List[(String, String)] = List(("Boston", "MA"), ("Dallas", "TX"), 
        ("Reston", "VA"), ("Orlando", "FL"), ("Seattle", "WA"))
    val webSites = Seq("www.amazon.com", "walmart.com", "target.com", "shopify.com")
    val failureReasons = Seq("Invalid account number", "Cannot reach bank", "Invalid expiry date", "Invalid CVV")
    val statuses = Seq("Y", "N")

    def generateRecords(numberOfRecords: Int) : Seq[Domain.OrderRecord] = {
        val random = new Random()

        val orderRecords = (1 to numberOfRecords).map{ r =>
        val customer = getRandomItemFromList(customers)
        val customerId = customer._1
        val customerName = customer._2
        val product = getRandomProduct(products)
        val productId = product._1
        val productName = product._2
        var productCategory = product._3
        var paymentType = getRandomItemFromSeq(paymentTypes)
        val cityCountry = getRandomItemFromListStrings(citiesCountries)
        val city = cityCountry._1
        val country = cityCountry._2
        val webSite = getRandomItemFromSeq(webSites)
        val orderId = randomAlphaNumericString(10)
        val orderDate = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now)
        val quantity = 1 + random.nextInt(10)
        val price = product._4
        val paymentTxnId = randomAlphaNumericString(10)
        val status = getRandomItemFromSeq(statuses)
        val failureReason = if(status == "N") getRandomItemFromSeq(failureReasons) else "N/A"

        import java.sql.Timestamp
        val time = new Timestamp(System.currentTimeMillis())

        Domain.OrderRecord(
            order_id = orderId, 
            datetime = orderDate,
            customer_id = customerId,
            customer_name = customerName,
            product_id = productId,
            product_name = productName,
            product_category = productCategory,
            qty = quantity,
            price = price,
            amount = quantity * price,
            city = city,
            country = country,
            ecommerce_website_name = webSite,
            payment_type = paymentType,
            payment_txn_id = paymentTxnId,
            payment_txn_success = status,
            failure_reason = failureReason,
            transactionTimestamp = time
        )
    }

    orderRecords // Return this.
    }

    def getRandomItemFromSeq(collection: Seq[String]) : String = {
        val random = new Random
        val randItem = collection(random.nextInt(collection.length))
        return randItem
    }

    def getRandomProduct(collection: List[(Int, String, String, Double)]) : (Int, String, String, Double) = {
        val random = new Random
        val randItem = collection(random.nextInt(collection.length))
        return randItem
    }

    def getRandomItemFromList(collection: List[(Int, String)]) : (Int, String) = {
        val random = new Random
        val randItem = collection(random.nextInt(collection.length))
        return randItem
    }

    def getRandomItemFromListStrings(collection: List[(String, String)]) : (String, String) = {
        val random = new Random
        val randItem = collection(random.nextInt(collection.length))
        return randItem
    }

    def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
        val sb = new StringBuilder
        for (i <- 1 to length) {
        val randomNum = util.Random.nextInt(chars.length)
        sb.append(chars(randomNum))
        }
        sb.toString
    }

    def randomAlphaNumericString(length: Int): String = {
        //val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        val chars = ('A' to 'Z') ++ ('0' to '9')
        randomStringFromCharList(length, chars)
    }
}