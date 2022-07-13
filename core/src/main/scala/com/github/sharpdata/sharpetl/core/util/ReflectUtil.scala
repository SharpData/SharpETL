package com.github.sharpdata.sharpetl.core.util

import scala.reflect.runtime.universe.TermName
import scala.reflect.runtime.{universe => ru}


object ReflectUtil {
  val classMirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

  def reflectObjectMethod(className: String, methodName: String, args: Any*): Any = {
    val moduleSymbol = classMirror.staticModule(className)
    val moduleMirror = classMirror.reflectModule(moduleSymbol)
    val instanceMirror = classMirror.reflect(moduleMirror.instance)
    val methodSymbol = moduleSymbol
      .typeSignature
      .members
      .filter(member => {
        member.isMethod &&
          member.name == TermName(methodName) &&
          member.asMethod.paramLists.nonEmpty &&
          member.asMethod.paramLists.head.size == args.size
      })
      .head
      .asMethod
    instanceMirror.reflectMethod(methodSymbol)(args: _*)
  }

}
