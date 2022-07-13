package com.github.sharpdata.sharpetl.core.util

import org.apache.log4j.Logger

object ETLLogger extends Serializable {
  val logger: Logger = Logger.getLogger("ETLLogger")

  @inline def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  @inline def info(msg: String): Unit = {
    logger.info(msg)
  }

  @inline def error(msg: String): Unit = {
    logger.error(msg)
  }

  @inline def error(msg: String, t: Throwable): Unit = {
    logger.error(msg, t)
  }

  @inline def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  @inline def fatal(msg: String): Unit = {
    logger.fatal(msg)
  }

  @inline def fatal(msg: String, t: Throwable): Unit = {
    logger.fatal(msg, t)
  }
}
