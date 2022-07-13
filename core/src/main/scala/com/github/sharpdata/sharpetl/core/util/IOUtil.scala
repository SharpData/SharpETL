package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.Constants.Encoding

import java.io.{DataInputStream, File, FileInputStream, FileWriter, InputStream, PrintWriter}
import java.nio.charset.CodingErrorAction
import java.util.jar.JarFile
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Codec, Source}

object IOUtil {

  def delete(path: String): Unit = {
    val file = new File(path)
    if (file.exists()) {
      file.delete()
    }
  }

  def mkdirs(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      file.mkdirs()
    }
  }

  def write(path: String, line: String, append: Boolean = false): Unit = {
    val file = new File(path)
    val writer = new PrintWriter(new FileWriter(file, append))
    writer.println(line)
    writer.close()
  }

  def readFile(path: String): InputStream = {
    try {
      new FileInputStream(path)
    } catch {
      case e: Throwable => throw new RuntimeException(s"unable to read file from $path", e)
    }
  }

  def getBytesDataReader(path: String): DataInputStream = {
    new DataInputStream(readFile(path))
  }

  def readLinesFromText(path: String, charset: String = Encoding.UTF8): List[String] = {
    try {
      val source = Source.fromFile(path, charset)
      val input = source.getLines().toList
      source.close()
      input
    } catch {
      case _: java.io.FileNotFoundException => readProcessConfigFromJar(path)
    }
  }

  def recursiveListFiles(path: String): ArrayBuffer[String] = {
    val list = ArrayBuffer[String]()
    val file = new File(path)
    if (file != null && file.exists()) {
      if (file.isDirectory) {
        list ++= file.listFiles().flatMap(f => recursiveListFiles(f.getPath))
      } else {
        list += file.getPath
      }
    }
    list
  }

  def listFiles(pathName: String): List[String] = {
    val list = ArrayBuffer[String]()
    val absolutePaths = this.getClass.getClassLoader.getResources(pathName)
    while (absolutePaths.hasMoreElements) {
      val resourcePath = absolutePaths.nextElement().getPath
      list ++= recursiveListFiles(resourcePath).toList
    }
    list.toList
  }

  def listFilesJar(configRootDir: String): List[String] = {
    val path = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val jarFile = new File(path)
    if (jarFile.isDirectory) {
      // scalastyle:off
      return List()
      // scalastyle:on
    }
    val localJarFile = new JarFile(jarFile)
    val entries = localJarFile.entries()
    val result = mutable.ListBuffer[String]()
    while (entries.hasMoreElements) {
      val jarEntry = entries.nextElement()
      val innerPath = jarEntry.getName
      if (innerPath.startsWith(configRootDir)) {
        result.append(innerPath)
      }
    }
    result.toList
  }

  def recursiveListFilesFromResource(pathName: String): List[String] = {
    (listFiles(pathName) ++ listFilesJar(pathName))
      .filter(it => it.contains(".sql") || it.contains(".scala"))
  }

  def readLinesFromInputStream(inputStream: InputStream): List[String] = {
    assert(inputStream != null)
    var source: Option[BufferedSource] = None
    try {
      implicit val codec: Codec = Codec(Encoding.UTF8)
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      source = Option(Source.fromInputStream(inputStream))
      val input = source.get.getLines().toList
      input
    } catch {
      case e: Exception =>
        ETLLogger.error(s"read file failed.")
        throw e
    } finally {
      if (source.isDefined) {
        try {
          source.get.close()
        } catch {
          case e: Exception =>
            ETLLogger.error(s"close BufferedSource failed.", e)
        }
      }
    }
  }

  def readLinesFromResource(path: String): List[String] = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(path)
    readLinesFromInputStream(inputStream)
  }

  def getInputStreamFromJar(filePath: String): InputStream = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(
      filePath
    )
    if (inputStream == null) {
      throw new RuntimeException(s"Not found file '$filePath' in jar.")
    }
    inputStream
  }

  def readProcessConfigFromJar(fileName: String): List[String] = {
    val inputStream = getInputStreamFromJar(fileName)
    readLinesFromInputStream(inputStream)
  }

  def getFullPath(path: String): String =
    if (path.startsWith("~")) {
      //user home dir
      val home: String = System.getProperty("user.home")
      home + path.replaceFirst("~", "")
    } else {
      path
    }

}
