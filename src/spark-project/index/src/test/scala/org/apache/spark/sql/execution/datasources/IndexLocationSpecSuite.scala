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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path

import com.github.lightcopy.testutil.UnitTestSuite
import com.github.lightcopy.testutil.implicits._

/** Test location spec for validation */
private[datasources] case class TestLocationSpec(
    unresolvedIndentifier: String,
    unresolvedDataspace: String,
    sourcePath: Path) extends IndexLocationSpec

class IndexLocationSpecSuite extends UnitTestSuite {
  test("IndexLocationSpec - empty/null identifier") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec(null, "value", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Empty identifier"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("", "value", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Empty identifier"))
  }

  test("IndexLocationSpec - empty/null dataspace") {
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", null, new Path(".")).dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))

    err = intercept[IllegalArgumentException] {
      TestLocationSpec("value", "", new Path(".")).dataspace
    }
    assert(err.getMessage.contains("Empty dataspace"))
  }

  test("IndexLocationSpec - invalid set of characters") {
    // identifier contains spaces
    var err = intercept[IllegalArgumentException] {
      TestLocationSpec("test ", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character   in identifier test "))

    // all characters are invalid
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("#$%", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character # in identifier #$%"))

    // identifier contains uppercase characters
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("Test", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character T in identifier Test"))

    // identifier contains underscore
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test_123", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character _ in identifier test_123"))

    // identifier contains hyphen
    err = intercept[IllegalArgumentException] {
      TestLocationSpec("test-123", "ok", new Path(".")).identifier
    }
    assert(err.getMessage.contains("Invalid character - in identifier test-123"))
  }

  test("IndexLocationSpec - valid identifier") {
    // test identifier
    TestLocationSpec("test", "test", new Path(".")).identifier
    TestLocationSpec("test1239", "test1239", new Path(".")).identifier
    TestLocationSpec("012345689", "012345689", new Path(".")).identifier
    TestLocationSpec("0123test", "0123test", new Path(".")).identifier
    // test dataspace
    TestLocationSpec("test", "test", new Path(".")).dataspace
    TestLocationSpec("test1239", "test1239", new Path(".")).dataspace
    TestLocationSpec("012345689", "012345689", new Path(".")).dataspace
    TestLocationSpec("0123test", "0123test", new Path(".")).dataspace
    // test source path
    TestLocationSpec("a", "b", new Path("/tmp/test")).sourcePath should be (new Path("/tmp/test"))
  }

  test("IndexLocationSpec - empty path") {
    val err = intercept[IllegalArgumentException] {
      TestLocationSpec("a", "b", new Path(""))
    }
    assert(err.getMessage.contains("Can not create a Path from an empty string"))
  }

  test("IndexLocationSpec - source spec") {
    val spec = SourceLocationSpec("parquet", new Path("."))
    spec.identifier should be ("parquet")
    spec.dataspace should be ("source")
    spec.sourcePath should be (new Path("."))
  }

  test("IndexLocationSpec - catalog spec") {
    val spec = CatalogLocationSpec("parquet", new Path("."))
    spec.identifier should be ("parquet")
    spec.dataspace should be ("catalog")
    spec.sourcePath should be (new Path("."))
  }

  test("IndexLocationSpec - toString") {
    TestLocationSpec("identifier", "dataspace", new Path("/tmp/test")).toString should be (
      "[dataspace/identifier, source=/tmp/test]")
    SourceLocationSpec("parquet", new Path("/tmp/source")).toString should be (
      "[source/parquet, source=/tmp/source]")
    CatalogLocationSpec("parquet", new Path("/tmp/catalog")).toString should be (
      "[catalog/parquet, source=/tmp/catalog]")
  }
}
