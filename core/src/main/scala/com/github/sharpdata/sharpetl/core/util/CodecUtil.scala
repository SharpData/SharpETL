package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.Constants.IO_COMPRESSION_CODEC_CLASS.{GZC_CODEC_CLASS, GZ_CODEC_CLASS}

object CodecUtil {
  def matchCodec(extension: String): Option[String] = {
    extension match {
      case ".gz.c" => Some(GZC_CODEC_CLASS)
      case ".gz" => Some(GZ_CODEC_CLASS)
      case _ => None
    }
  }
}
