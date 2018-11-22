/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.github.lightcopy.testutil

import java.io.{InputStream, OutputStream}
import java.util.UUID

import org.apache.hadoop.conf.{Configuration => HadoopConf}
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.sql.{DataFrame, Row}

import com.github.lightcopy.testutil.implicits._

trait TestBase {
  val RESOLVER = "path-resolver"

  var path: String = ""

  // local file system for tests
  val fs = FileSystem.get(new HadoopConf(false))

  /** returns raw path of the folder where it finds resolver */
  private def getRawPath(): String = {
    if (path.isEmpty) {
      path = getClass.getResource("/" + RESOLVER).getPath()
    }
    path
  }

  /** base directory of the project */
  final protected def baseDirectory(): String = {
    val original = getRawPath().split("/")
    require(original.length > 4, s"Path length is too short (<= 4): ${original.length}")
    val base = original.dropRight(4)
    var dir = ""
    for (suffix <- base) {
      if (suffix.nonEmpty) {
        dir = dir / suffix
      }
    }
    dir
  }

  /** main directory of the project (./src/main) */
  final protected def mainDirectory(): String = {
    baseDirectory() / "src" / "main"
  }

  /** test directory of the project (./src/test) */
  final protected def testDirectory(): String = {
    baseDirectory() / "src" / "test"
  }

  /** target directory of the project (./target) */
  final protected def targetDirectory(): String = {
    baseDirectory() / "target"
  }

  /** Create directories for path recursively */
  final protected def mkdirs(path: String): Boolean = {
    mkdirs(new HadoopPath(path))
  }

  final protected def mkdirs(path: HadoopPath): Boolean = {
    fs.mkdirs(path)
  }

  /** Create empty file, similar to "touch" shell command, but creates intermediate directories */
  final protected def touch(path: String): Boolean = {
    touch(new HadoopPath(path))
  }

  final protected def touch(path: HadoopPath): Boolean = {
    fs.mkdirs(path.getParent)
    fs.createNewFile(path)
  }

  /** Delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: String, recursive: Boolean): Boolean = {
    rm(new HadoopPath(path), recursive)
  }

  /** Delete directory / file with path. Recursive must be true for directory */
  final protected def rm(path: HadoopPath, recursive: Boolean): Boolean = {
    fs.delete(path, recursive)
  }

  /** Open file for a path */
  final protected def open(path: String): InputStream = {
    open(new HadoopPath(path))
  }

  final protected def open(path: HadoopPath): InputStream = {
    fs.open(path)
  }

  /** Create file with a path and return output stream */
  final protected def create(path: String): OutputStream = {
    create(new HadoopPath(path))
  }

  final protected def create(path: HadoopPath): OutputStream = {
    fs.create(path)
  }

  /** Compare two DataFrame objects */
  final protected def checkAnswer(df: DataFrame, expected: DataFrame): Unit = {
    val got = df.collect.map(_.toString).sortWith(_ < _)
    val exp = expected.collect.map(_.toString).sortWith(_ < _)
    assert(got.sameElements(exp), s"Failed to compare DataFrame ${got.mkString("[", ", ", "]")} " +
      s"with expected input ${exp.mkString("[", ", ", "]")}")
  }

  final protected def checkAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    val sc = df.sqlContext.sparkContext
    checkAnswer(df, df.sqlContext.createDataFrame(sc.parallelize(expected), df.schema))
  }

  /** Create temporary directory on local file system */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "lightcopy"): HadoopPath = {
    val dir = new HadoopPath(root / namePrefix / UUID.randomUUID().toString)
    fs.mkdirs(dir)
    dir
  }

  /** Execute block of code with temporary hadoop path and path permission */
  private def withTempHadoopPath(path: HadoopPath, permission: Option[FsPermission])
      (func: HadoopPath => Unit): Unit = {
    try {
      if (permission.isDefined) {
        fs.setPermission(path, permission.get)
      }
      func(path)
    } finally {
      fs.delete(path, true)
    }
  }


  /** Execute code block with created temporary directory with provided permission */
  def withTempDir(permission: FsPermission)(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), Some(permission))(func)
  }

  /** Execute code block with created temporary directory */
  def withTempDir(func: HadoopPath => Unit): Unit = {
    withTempHadoopPath(createTempDir(), None)(func)
  }
}
