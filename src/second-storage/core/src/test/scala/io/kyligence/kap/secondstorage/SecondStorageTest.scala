/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */
package io.kyligence.kap.secondstorage

import java.lang.reflect.InvocationTargetException
import java.util.ArrayList

import io.kyligence.kap.metadata.cube.model.NDataflow
import io.kyligence.kap.metadata.model.NDataModel
import io.kyligence.kap.secondstorage.enums.LockTypeEnum
import org.apache.kylin.common.{ForceToTieredStorage, QueryContext}
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.mockito.Mockito
import scala.reflect.runtime.{universe => ru}

class SecondStorageTest extends SparderBaseFunSuite with LocalMetadata {

  test("SecondStorage.enabled should not throw execution") {
    overwriteSystemProp("kylin.second-storage.class", "X")
    assertResult(false)(SecondStorage.enabled)

    SecondStorage.init(false)
    assertResult(false)(SecondStorage.enabled)
    assertResult(null)(SecondStorage.load("io.kyligence.kap.secondstorage.metadata.TableFlow"))

    val testStorage = SecondStorage.load(classOf[TestStorage].getCanonicalName)
    assertResult(true)(testStorage != null)
    assertResult(true)(testStorage.isInstanceOf[TestStorage])
  }

  test("SecondStorage.configLoader") {
    val thrown = intercept[RuntimeException] {
      SecondStorage.configLoader
    }
    assert(thrown.getMessage === "second storage plugin is null")
  }

  def invokeObjectPrivateMethod[R](methodName: String, args: AnyRef*): R = {
    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val instanceMirror = rm.reflect(SecondStorage)
    val methodSymbol = ru.typeOf[SecondStorage.type].decl(ru.TermName(methodName)).asMethod
    val method = instanceMirror.reflectMethod(methodSymbol)
    method(args: _*).asInstanceOf[R]
  }

  test("SecondStorage.throwException") {
    assertResult(None)(invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null))

    ForceToTieredStorage.values().foreach(f => {
      QueryContext.current().setForcedToTieredStorage(f)
      if (f == ForceToTieredStorage.CH_FAIL_TO_DFS) {
        assertResult(None)(invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null))
      } else {
        val thrown = intercept[InvocationTargetException] {
          invokeObjectPrivateMethod("tryCreateDataFrame", null, null, null, null)
        }
      }
    })
  }

  test("SecondStorage.lock1") {
    var dataModel: NDataModel = Mockito.mock(classOf[NDataModel])
    var dataflow: NDataflow = Mockito.mock(classOf[NDataflow])
    Mockito.when(dataflow.getProject).thenReturn("project")
    Mockito.when(dataflow.getModel).thenReturn(dataModel)
    Mockito.when(dataModel.getId).thenReturn("modelid")
    var listString = new ArrayList[String](2)
    listString.add(0, "a")
    listString.add(1, "b")

    val utility1 = Mockito.mockStatic(classOf[SecondStorageUtil])
    utility1.when(() => SecondStorageUtil.isModelEnable("project", "modelid")).thenReturn(true)
    assertResult(true)(SecondStorageUtil.isModelEnable("project", "modelid"))
    utility1.when(() => SecondStorageUtil.getProjectLocks("project")).thenReturn(listString)

    val utility2 = Mockito.mockStatic(classOf[LockTypeEnum])
    utility2.when(() => LockTypeEnum.locked(LockTypeEnum.QUERY.name(), listString)).thenReturn(true)

    assertResult(Option.empty)(SecondStorage.trySecondStorage(null, dataflow, null, ""))
    utility1.close()
    utility2.close()
  }

  test("SecondStorage.lock2") {
    var dataModel: NDataModel = Mockito.mock(classOf[NDataModel])
    var dataflow: NDataflow = Mockito.mock(classOf[NDataflow])
    Mockito.when(dataflow.getProject).thenReturn("project")
    Mockito.when(dataflow.getModel).thenReturn(dataModel)
    Mockito.when(dataModel.getId).thenReturn("modelid")
    var listString = new ArrayList[String](2)
    listString.add(0, "a")
    listString.add(1, "b")

    val utility1 = Mockito.mockStatic(classOf[SecondStorageUtil])
    utility1.when(() => SecondStorageUtil.isModelEnable("project", "modelid")).thenReturn(true)
    assertResult(true)(SecondStorageUtil.isModelEnable("project", "modelid"))
    utility1.when(() => SecondStorageUtil.getProjectLocks("project")).thenReturn(listString)

    val utility2 = Mockito.mockStatic(classOf[LockTypeEnum])
    utility2.when(() => LockTypeEnum.locked(LockTypeEnum.QUERY.name(), listString)).thenReturn(false)
    assertResult(None)(SecondStorage.trySecondStorage(null, dataflow, null, ""))

    utility1.close()
    utility2.close()
  }
}
