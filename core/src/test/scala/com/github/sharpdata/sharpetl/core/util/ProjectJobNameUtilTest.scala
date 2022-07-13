package com.github.sharpdata.sharpetl.core.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ProjectJobNameUtilTest extends AnyFlatSpec with should.Matchers {

  it should "get project name by job name correctly when project name exist" in {

    val path = getClass.getResource("/application.properties").toString
    ETLConfig.setPropertyPath(path)

    assert(ProjectJobNameUtil.getProjectName("job1") == "project1")
  }

  it should "get project name by job name correctly when project name not exist" in {

    val path = getClass.getResource("/application.properties").toString
    ETLConfig.setPropertyPath(path)

    assert(ProjectJobNameUtil.getProjectName("job5") == "")
  }

}
