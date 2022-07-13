package com.github.sharpdata.sharpetl.datasource.kafka

import com.github.sharpdata.sharpetl.core.util.ETLConfig
import KafkaConfig.buildNativeKafkaProducerConfig
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should

class ETLKafkaConfigSpec extends AnyFunSpec with BeforeAndAfterEach with should.Matchers {
  it("should read plain text password from encrypted text") {
    val filePath = getClass.getResource("/application.properties_encrypted").toString
    ETLConfig.setPropertyPath(filePath)

    val props = buildNativeKafkaProducerConfig("fake-group-id")
    props("ssl.truststore.password") should be("plain text password: 1qaz@WSX")
  }


  override protected def beforeEach(): Unit = {
    reinitializeETLConfig()
    super.beforeEach()
  }

  // trick to reinitialize object, each test starts with a fresh properties
  private def reinitializeETLConfig() = {
    val cons = ETLConfig.getClass.getDeclaredConstructor()
    cons.setAccessible(true)
    cons.newInstance()
  }
}
