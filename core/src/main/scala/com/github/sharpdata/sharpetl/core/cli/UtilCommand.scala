package com.github.sharpdata.sharpetl.core.cli

import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import org.apache.log4j.Level
import picocli.CommandLine
import picocli.CommandLine.Parameters


@CommandLine.Command(name = "encrypt", description = Array("Encrypts the given string using the given key."))
class EncryptionCommand extends UtilCommand {

  @CommandLine.Option(names = Array("-p", "--propertyFilePath"), description = Array("property file path"), required = true)
  var propertyFilePath: String = _

  @Parameters(index = "0", description = Array("target content"), defaultValue = "")
  var content: String = _

  override def run(): Unit = {
    super.run()
    ETLLogger.info(s"Original content is $content")
    ETLConfig.setPropertyPath(propertyFilePath)
    val encryptor = ETLConfig.encryptor.get
    val encryptedContent = encryptor.encrypt(content)
    ETLLogger.info(s"ENC($encryptedContent)")
  }

}

class UtilCommand extends Runnable {
  @CommandLine.Option(
    names = Array("-h", "--help"), usageHelp = true, description = Array("Display this help message")
  )
  var helpRequested = false

  override def run(): Unit = {
    if (helpRequested) CommandLine.usage(this, System.out)
    import org.apache.log4j.Logger
    Logger.getRootLogger.setLevel(Level.OFF)
  }
}
