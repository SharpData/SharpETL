package com.github.sharpdata.sharpetl.core.repository

import org.apache.ibatis.io.Resources
import org.apache.ibatis.session.{SqlSession, SqlSessionFactory, SqlSessionFactoryBuilder}

object MyBatisSession {
  private var sqlSessionFactory: SqlSessionFactory = getFactory()

  private def getFactory() = {
    val resource = s"mybatis-config.xml"
    val inputStream = Resources.getResourceAsStream(resource)

    new SqlSessionFactoryBuilder().build(inputStream)
  }

  // for test
  def reloadFactory(): Unit = {
    sqlSessionFactory = getFactory()
  }

  def execute[T](query: SqlSession => T): T = {
    var session: Option[SqlSession] = None
    try {
      session = Some(sqlSessionFactory.openSession(true))
      val sessionValue = session.get
      query(sessionValue)
    }
    finally {
      if (session.nonEmpty) {
        session.get.close()
      }
    }
  }
}
