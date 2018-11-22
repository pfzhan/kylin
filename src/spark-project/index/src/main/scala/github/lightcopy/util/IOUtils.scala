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

package com.github.lightcopy.util

import java.io.{InputStream, OutputStream}

import org.apache.commons.io.{IOUtils => CommonIOUtils}
import org.apache.hadoop.fs.{FileSystem, Path}

/** IO related utility methods */
object IOUtils {
  /** Read content as UTF-8 String from provided file */
  def readContent(fs: FileSystem, path: Path): String = {
    val in = fs.open(path)
    try {
      CommonIOUtils.toString(in, "UTF-8")
    } finally {
      in.close()
    }
  }

  /** Read content from stream */
  def readContentStream(fs: FileSystem, path: Path)(func: InputStream => Unit): Unit = {
    val in = fs.open(path)
    try {
      func(in)
    } finally {
      in.close()
    }
  }

  /** Write content as UTF-8 String into provided file path, file is ovewritten on next attempt */
  def writeContent(fs: FileSystem, path: Path, content: String): Unit = {
    val out = fs.create(path, true)
    try {
      CommonIOUtils.write(content, out, "UTF-8")
    } finally {
      out.close()
    }
  }

  /** Write content into stream, file is overwritten on next attempt */
  def writeContentStream(fs: FileSystem, path: Path)(func: OutputStream => Unit): Unit = {
    val out = fs.create(path, true)
    try {
      func(out)
    } finally {
      out.close()
    }
  }
}
