package com.github.sharpdata.sharpetl.core.util

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.exception.Exception.CanNotLoadPropertyFileException
import com.github.sharpdata.sharpetl.core.util.Constants.{Encoding, Environment}
import org.apache.commons.codec.binary.Base64
import org.jasypt.encryption.StringEncryptor
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor
import org.jasypt.properties.EncryptableProperties

import java.io.Reader
import java.nio.charset.CodingErrorAction
import java.util.Properties
import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.jdk.CollectionConverters._

object ETLConfig {

  private var prop: Option[Properties] = None

  var extraParam: mutable.Map[String, String] = mutable.Map()

  private var propertyPath: String = s"/application${StringUtil.environmentSuffix}.properties"

  private def rawProperties: Properties = {
    try {
      val p = new Properties()
      ETLLogger.info(s"Read raw properties $propertyPath")
      p.load(getPropertiesInputStream)
      replaceEnvVariable(p)
      p.asScala ++= (extraParam)
      p
    } catch {
      case e: Throwable =>
        throw CanNotLoadPropertyFileException(s"$propertyPath cannot be loaded", e)
    }
  }

  def encryptor: Option[StringEncryptor] = {
    val rawProp = rawProperties
    if (rawProp.containsKey("encrypt.algorithm")) {
      val encryptor = new StandardPBEStringEncryptor
      encryptor.setAlgorithm(rawProp.getOrDefault("encrypt.algorithm", "PBEWithMD5AndDES").toString)
      encryptor.setPassword(new String(Base64.decodeBase64(getEncryptKey), "UTF-8"))
      Some(encryptor)
    } else {
      None
    }
  }

  private def getEncryptKey: String = {
    var keyPath = rawProperties.getProperty("encrypt.keyPath", "")
    if ("".equals(keyPath)) {
      keyPath = this.getClass.getClassLoader.getResource("etl.key").getPath
    }
    val offset = rawProperties.getProperty("encrypt.offset", "10").toInt
    val reader = if (keyPath.contains("hdfs")) {
      HDFSUtil.getBytesDataReader(keyPath)
    } else {
      IOUtil.getBytesDataReader(keyPath)
    }

    val len = reader.readInt()
    val bytesBuffer = new Array[Byte](len)
    for (index <- 0 until len) bytesBuffer(index) = (reader.readInt() - offset).toByte
    reader.close()

    new String(bytesBuffer, "UTF-8").reverse

  }

  def plainProperties: Map[String, String] = {
    properties.asScala.map { case (key, _) =>
      (
        key,
        properties.getProperty(key) //NOTE: we must get key so we could get plain text properties value
      )
    }.toMap
  }

  /**
   * Call [[ETLConfig.getProperty]] to get plain/decrypted value.
   * If you call `properties.asScala.filter/toList/etc` you may got encrypted value.
   * If you want all plain text values or you want to filter properties by your prefix, please using [[ETLConfig.plainProperties]].
   */
  private def properties: Properties = {
    if (prop.isDefined) {
      prop.get
    } else {
      val p = encryptor.map(strEncryptor => new EncryptableProperties(strEncryptor)).orElse(Some(new Properties())).get
      ETLLogger.info(s"Read properties $propertyPath")
      try {
        p.load(getPropertiesInputStream)
        replaceEnvVariable(p)
        p.asScala ++= (extraParam)
        prop = Some(p)
        p
      } catch {
        case e: Throwable =>
          throw CanNotLoadPropertyFileException(s"$propertyPath cannot be loaded", e)
      }
    }
  }

  def reInitProperties(): Unit = {
    prop = None
    extraParam = mutable.Map()
  }

  private def getPropertiesInputStream: Reader = {
    implicit val codec: Codec = Codec(Encoding.UTF8)
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val source =
      if (propertyPath.toLowerCase.startsWith("file")) {
        Source.fromURL(propertyPath)
      } else if (propertyPath.toLowerCase.startsWith("hdfs")) {
        Source.fromInputStream(HDFSUtil.readFile(propertyPath))
      } else {
        Source.fromURL(getClass.getResource(propertyPath))
      }
    source.reader()
  }

  private def replaceEnvVariable(prop: Properties): Unit = {
    val env = System.getenv()
    prop.asScala.foreach(tuple => {
      if (tuple._2.startsWith("$")) {
        prop.put(tuple._1, env.get(tuple._2.substring(1)))
      }
    })
  }

  def getHttpProperties(connectionName: String): Map[String, String] = {
    val prefix = s"$connectionName.http."
    plainProperties
      .filter(_._1.startsWith(prefix))
      .map { case (key, value) => key.substring(prefix.length, key.length) -> value }
  }

  def getSparkProperties(jobName: String): Map[String, String] = {
    getProperties("spark", "default") ++ getProperties("spark", jobName)
  }

  def getKafkaProperties: String = {
    properties.asScala.getOrElse("kafka.restapi", "")
  }

  def getProperties(paramType: String, jobName: String): Map[String, String] = {
    val prefix = s"$paramType.$jobName."
    plainProperties
      .filter(_._1.startsWith(prefix))
      .map { case (key, value) => key.substring(prefix.length, key.length) -> value }
  }

  def setPropertyPath(path: String, env: String = ""): Unit = {
    Environment.current = env
    if (!isNullOrEmpty(path)) {
      propertyPath = path
    } else {
      propertyPath = s"/application${StringUtil.environmentSuffix}.properties"
    }
    prop = None
  }

  def getProperty(key: String): String = {
    val value = this.properties.getProperty(key)
    if (isNullOrEmpty(value)) {
      ETLLogger.error(s"[Config] property key $key was not found in properties file!")
    }
    value
  }

  def getProperties(pathPrefix: String): Map[String, String] = {
    val prefix = s"$pathPrefix."
    plainProperties
      .filter(_._1.startsWith(prefix))
      .map { case (key, value) => key.substring(prefix.length, key.length) -> value }
  }

  def getProperty(key: String, defaultValue: String): String = {
    this.properties.getProperty(key, defaultValue)
  }

  lazy val jobIdColumn: String = ETLConfig.getProperty("etl.default.jobId.column", "job_id")
  lazy val jobTimeColumn: String = ETLConfig.getProperty("etl.default.jobTime.column", "job_time")
  lazy val partitionColumn: String = ETLConfig.getProperty("etl.default.partition.column", "dt")
  lazy val incrementalDiffModeDataLimit: String = ETLConfig.getProperty("etl.default.incrementalDiff.limit", "5000000")
  lazy val deltaLakeBasePath: String = ETLConfig.getProperty("etl.default.delta.base.path", "/mnt/delta/")
  lazy val purgeHiveTable: String = ETLConfig.getProperty("etl.default.purgeHiveTable", "none")
}
