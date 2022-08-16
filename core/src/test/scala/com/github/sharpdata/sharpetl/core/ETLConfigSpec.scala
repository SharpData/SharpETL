package com.github.sharpdata.sharpetl.core

import com.github.sharpdata.sharpetl.core.util.{Constants, ETLConfig}
import com.github.sharpdata.sharpetl.core.util.Constants.Environment

import java.io.{DataOutputStream, FileOutputStream}
import com.github.sharpdata.sharpetl.core.util.{Constants, ETLConfig, IOUtil, StringUtil}
import org.apache.commons.codec.binary.Base64
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class ETLConfigSpec extends AnyFunSpec with BeforeAndAfterEach {

  ignore("hdfs") {
    it("should read from hdfs") {
      Environment.CURRENT = Constants.Environment.LOCAL
      ETLConfig.setPropertyPath("hdfs:///tmp/application.properties")
      val res = ETLConfig.getProperty("from_hdfs_path")
      assert(!StringUtil.isNullOrEmpty(res))
      assert(res == "true")
    }
  }

  it("read from class path") {
    Constants.Environment.CURRENT = Constants.Environment.LOCAL
    val path = ETLConfig.getProperty("etl.workflow.path")
    assert(!StringUtil.isNullOrEmpty(path))
  }

  it("read from file path") {
    Constants.Environment.CURRENT = Constants.Environment.LOCAL
    val filePath = getClass.getResource("/application.properties_bak").toString
    ETLConfig.setPropertyPath(filePath)
    val res = ETLConfig.getProperty("from_file_path")
    assert(!StringUtil.isNullOrEmpty(res))
    assert("true" == res)
  }

  it("should encrypt password") {
    val filePath = getClass.getResource("/application.properties_encrypted").toString
    ETLConfig.setPropertyPath(filePath)
    val encryptor = ETLConfig.encryptor.get
    val password = "plain text password: 1qaz@WSX"
    val encryptedPassword = encryptor.encrypt(password)
    println(s"ENC($encryptedPassword)")
    assert(encryptor.decrypt(encryptedPassword) == password)
  }

  it("base64") {
    println(Base64.encodeBase64String("fMY$Vmbc#D3k".getBytes))
  }

  it("read from encrypted file") {
    val filePath = getClass.getResource("/application.properties_encrypted").toString
    ETLConfig.setPropertyPath(filePath)
    val res = ETLConfig.getProperty("some.password")
    assert(!StringUtil.isNullOrEmpty(res))
    assert("plain text password: 1qaz@WSX" == res)
  }

  it("write bytes to file and read it out") {
    val filePath = getClass.getResource("/application.properties_encrypted").toString
    ETLConfig.setPropertyPath(filePath)
    var path = this.getClass.getClassLoader.getResource("application.properties").getPath
    path = path.replace("application.properties", "my.key")
    print(path)
    val writer = new DataOutputStream(new FileOutputStream(path))
    val content = "XXXXX="
    val bytes = content.getBytes("UTF-8")
    writer.writeInt(bytes.length)
    bytes.reverse.foreach(x =>
      writer.writeInt(x + 10)
    )
    writer.flush()
    writer.close()
    val reader = IOUtil.getBytesDataReader(path)
    val len = reader.readInt()
    val bytesBuffer = new Array[Byte](len)
    for (index <- 0 until len) {
      bytesBuffer(index) = (reader.readInt() - 10).toByte
    }
    val contentFromFile = new String(bytesBuffer.reverse, "UTF-8")
    assert(content.equals(contentFromFile))
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
