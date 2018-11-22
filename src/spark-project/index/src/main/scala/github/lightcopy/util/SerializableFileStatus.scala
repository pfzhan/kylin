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

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}

/**
 * Serializable version of `BlocLocation` of HDFS.
 */
case class SerializableBlockLocation(
  names: Array[String],
  hosts: Array[String],
  offset: Long,
  length: Long)

/**
 * Serializable version of `FileStatus`of HDFS.
 */
case class SerializableFileStatus(
  path: String,
  length: Long,
  isDir: Boolean,
  blockReplication: Short,
  blockSize: Long,
  modificationTime: Long,
  accessTime: Long,
  blockLocations: Array[SerializableBlockLocation])

/**
 * Util to convert serializable file status into HDFS `LocatedFileStatus`. Currently file status is
 * assumed to have empty block locations, if it was serialized from non-`LocatedFileStatus`.
 */
object SerializableFileStatus {
  def fromFileStatus(status: FileStatus): SerializableFileStatus = {
    val blockLocations = status match {
      case f: LocatedFileStatus =>
        f.getBlockLocations.map { loc =>
          SerializableBlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
        }
      case _ =>
        Array.empty[SerializableBlockLocation]
    }

    SerializableFileStatus(
      status.getPath.toString,
      status.getLen,
      status.isDirectory,
      status.getReplication,
      status.getBlockSize,
      status.getModificationTime,
      status.getAccessTime,
      blockLocations)
  }

  def toFileStatus(status: SerializableFileStatus): FileStatus = {
    val blockLocations = status.blockLocations.map { loc =>
      new BlockLocation(loc.names, loc.hosts, loc.offset, loc.length)
    }

    new LocatedFileStatus(
      new FileStatus(
        status.length,
        status.isDir,
        status.blockReplication,
        status.blockSize,
        status.modificationTime,
        new Path(status.path)),
      blockLocations)
  }
}
