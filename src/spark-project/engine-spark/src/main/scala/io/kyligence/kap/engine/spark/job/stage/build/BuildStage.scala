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

package io.kyligence.kap.engine.spark.job.stage.build

import com.google.common.collect.{Queues, Sets}
import io.kyligence.kap.common.persistence.transaction.UnitOfWork
import io.kyligence.kap.common.persistence.transaction.UnitOfWork.Callback
import io.kyligence.kap.engine.spark.application.SparkApplication
import io.kyligence.kap.engine.spark.builder.DictionaryBuilderHelper
import io.kyligence.kap.engine.spark.job._
import io.kyligence.kap.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import io.kyligence.kap.engine.spark.job.stage.{BuildParam, StageExec}
import io.kyligence.kap.metadata.cube.cuboid.AdaptiveSpanningTree.TreeNode
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager.NIndexPlanUpdater
import io.kyligence.kap.metadata.cube.model._
import org.apache.commons.math3.ml.clustering.{Clusterable, KMeansPlusPlusClusterer}
import org.apache.commons.math3.ml.distance.EarthMoversDistance
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.util.Objects
import java.util.concurrent.{BlockingQueue, CountDownLatch, ForkJoinPool, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport


abstract class BuildStage(private val jobContext: SegmentJob,
                         private val dataSegment: NDataSegment,
                         private val buildParam: BuildParam)
  extends SegmentExec with StageExec {
  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = dataSegment

  override def getSegmentId: String = dataSegment.getId

  // Needed variables from job context.
  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val resourceContext = jobContext.getBuildContext
  protected final val runtime = jobContext.runtime
  protected final val readOnlyLayouts = jobContext.getReadOnlyLayouts

  // Needed variables from data segment.
  protected final val segmentId = dataSegment.getId
  protected final val project = dataSegment.getProject

  protected final val dataModel = dataSegment.getModel
  protected final val storageType = dataModel.getStorageType

  private lazy val spanningTree = buildParam.getSpanningTree

  private lazy val flatTableDesc = buildParam.getFlatTableDesc

  private lazy val flatTable = buildParam.getBuildFlatTable

  private lazy val flatTableDS: Dataset[Row] = buildParam.getFlatTable
  private lazy val flatTableStats: Statistics = buildParam.getFlatTableStatistics

  // thread unsafe
  private var cachedLayoutSanity: Option[Map[Long, Long]] = None
  // thread unsafe
  private val cachedLayoutDS = mutable.HashMap[Long, Dataset[Row]]()

  // thread unsafe
  private var cachedIndexInferior: Map[Long, InferiorCountDownLatch] = _

  protected val sparkSchedulerPool = "build"

  override protected def columnIdFunc(colRef: TblColRef): String = flatTableDesc.getColumnIdAsString(colRef)

  protected def buildLayouts(): Unit = {
    // Maintain this variable carefully.
    var remainingTaskCount = 0

    // Share failed task 'throwable' with main thread.
    val failQueue = Queues.newLinkedBlockingQueue[Option[Throwable]]()

    var successTaskCount = 0
    // Main loop: build layouts.
    while (spanningTree.nonSpanned()) {
      if (resourceContext.isAvailable) {
        // Drain immediately.
        drain()
        // Use the latest data segment.
        val segment = jobContext.getSegment(segmentId)
        val nodes = spanningTree.span(segment).asScala
        val tasks = nodes.flatMap(node => getLayoutTasks(segment, node))
        remainingTaskCount += tasks.size
        // Submit tasks, no task skip inside this foreach.
        tasks.foreach { task =>
          runtime.submit(() => try {
            // If unset
            setConfig4CurrentThread()
            // Build layout.
            buildLayout(task)
            // Offer 'Node' if everything was well.
            failQueue.offer(None)
          } catch {
            // Offer 'Throwable' if unexpected things happened.
            case t: Throwable => failQueue.offer(Some(t))
          })
        }
        if (0 != tasks.size) {
          KylinBuildEnv.get().buildJobInfos.recordCuboidsNumPerLayer(segmentId, tasks.size)

          val layoutCount = KylinBuildEnv.get().buildJobInfos.getSeg2cuboidsNumPerLayer.get(segmentId).asScala.sum
          onBuildLayoutSuccess(layoutCount)
        }
      }
      // Poll and fail fast.
      remainingTaskCount -= failFastPoll(failQueue, 3L, TimeUnit.SECONDS)
    }
    // Await or fail fast.
    while (remainingTaskCount > 0) {
      remainingTaskCount -= failFastPoll(failQueue)
    }
    // Drain immediately after all layouts built.
    drain()
  }

  protected final def failFastPoll(failQueue: BlockingQueue[Option[Throwable]], //
                                   timeout: Long = 1, unit: TimeUnit = TimeUnit.SECONDS): Int = {
    assert(unit.toSeconds(timeout) > 0, s"Timeout should be positive seconds to avoid a busy loop.")
    var count = 0
    var failure = failQueue.poll(timeout, unit)
    while (Objects.nonNull(failure)) {
      if (failure.nonEmpty) {
        logError(s"Fail fast.", failure.get)
        drain()
        throw failure.get
      }
      count += 1
      failure = failQueue.poll()
    }
    count
  }

  private def beforeBuildLayouts(): Unit = {
    // Build flat table?
    if (spanningTree.fromFlatTable()) {
      // Very very heavy step
      // Potentially global dictionary building & encoding within.
      // Materialize flat table.
      //      flatTableDS = flatTable.getFlatTableDS

      // Collect statistics for flat table.
      buildStatistics()

      // Build inferior flat table.
      if (config.isInferiorFlatTableEnabled) {
        buildInferior()
      }
    }

    // Build root node's layout sanity cache.
    buildSanityCache()

    // Cleanup potential temp data.
    cleanupLayoutTempData()
  }

  protected def buildStatistics(): Statistics = {
    flatTable.gatherStatistics()
  }

  protected def buildSanityCache(): Unit = {
    if (!config.isSanityCheckEnabled) {
      return
    }
    // Collect statistics for root nodes.
    val rootNodes = spanningTree.getRootNodes.asScala
    if (rootNodes.isEmpty) {
      return
    }

    logInfo(s"Segment $segmentId build sanity cache.")
    val parallel = rootNodes.map { node =>
      val layout = node.getLayout
      val layoutDS = getCachedLayoutDS(dataSegment, layout)
      (layout, layoutDS)
    }.par
    val forkJoinPool = new ForkJoinPool(rootNodes.size)
    try {
      parallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
      val sanityMap = parallel.map { case (layout, layoutDS) =>
        val sanityCount = SanityChecker.getCount(layoutDS, layout)
        (layout.getId, sanityCount)
      }.toMap.seq
      assert(sanityMap.keySet.size == rootNodes.size, //
        s"Collect sanity for root nodes went wrong: ${sanityMap.keySet.size} == ${rootNodes.size}")
      cachedLayoutSanity = Some(sanityMap)
      logInfo(s"Segment $segmentId finished build sanity cache $sanityMap.")
    } finally {
      forkJoinPool.shutdownNow()
    }
  }

  protected def cleanupLayoutTempData(): Unit = {
    logInfo(s"Segment $segmentId cleanup layout temp data.")
    val prefixes = readOnlyLayouts.asScala.map(_.getId).map(id => s"${id}_temp")
    val segmentPath = new Path(NSparkCubingUtil.getStoragePath(dataSegment))
    val fileSystem = segmentPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(segmentPath)) {
      return
    }
    val cleanups = fileSystem.listStatus(segmentPath, new PathFilter {
      override def accept(destPath: Path): Boolean = {
        val name = destPath.getName
        prefixes.exists(prefix => name.startsWith(prefix))
      }
    }).map(_.getPath)

    if (cleanups.isEmpty) {
      return
    }
    val processors = Runtime.getRuntime.availableProcessors
    val parallel = cleanups.par
    val forkJoinPool = new ForkJoinPool(Math.max(processors, cleanups.length / 2))
    try {
      parallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)
      parallel.foreach { p =>
        fileSystem.delete(p, true)
      }
    } finally {
      forkJoinPool.shutdownNow()
    }
    logInfo(s"Segment $segmentId cleanup layout temp data: ${cleanups.map(_.getName).mkString("[", ",", "]")}")
  }

  private def getLayoutTasks(segment: NDataSegment, node: TreeNode): Seq[LayoutBuildTask] = {
    val layouts = node.getLayouts.asScala // skip layouts
      .filterNot(layout => needSkipLayout(layout.getId, segment))
    if (layouts.isEmpty) {
      return Seq.empty
    }
    val sanityCount = getCachedLayoutSanity(node)
    if (node.parentIsNull) {
      // Build from flat table
      layouts.map { layout =>
        val ic = getCachedIndexInferior(layout.getIndex)
        val parentDS = if (ic.isDefined) ic.get.inferior else flatTableDS
        LayoutBuildTask(layout, None, parentDS, sanityCount, segment, ic)
      }
    } else {
      // Build from data layout
      val parentLayout = node.getParent.getLayout
      val parentDS = getCachedLayoutDS(segment, parentLayout)

      layouts.map { layout =>
        LayoutBuildTask(layout, Some(parentLayout), parentDS, sanityCount, segment)
      }
    }
  }

  private def getCachedLayoutSanity(node: TreeNode): Long = {
    // Not enabled.
    if (!config.isSanityCheckEnabled) {
      return SanityChecker.SKIP_FLAG
    }

    // From flat table.
    if (Objects.isNull(node.getRootNode)) {
      return flatTableStats.totalCount
    }

    // From data layout.
    if (cachedLayoutSanity.isEmpty) {
      return SanityChecker.SKIP_FLAG
    }
    val rootNode = node.getRootNode
    val cachedLayout = cachedLayoutSanity.get
    val layout = rootNode.getLayout
    assert(cachedLayout.contains(layout.getId), //
      s"Root node's layout sanity should have been cached: ${layout.getId}")
    cachedLayout(layout.getId)
  }

  private def getCachedLayoutDS(segment: NDataSegment, layout: LayoutEntity): Dataset[Row] = synchronized {
    cachedLayoutDS.getOrElseUpdate(layout.getId,
      // Or update, lightweight invocation.
      StorageStoreUtils.toDF(segment, layout, sparkSession))
  }

  // scala version < 2.13
  // which you could invoke with minOptionBy(seq)(_.something)
  private def minOptionBy[A, B: Ordering](seq: Seq[A])(f: A => B) =
    seq reduceOption Ordering.by(f).min

  sealed case class LayoutBuildTask(layout: LayoutEntity //
                                    , parentLayout: Option[LayoutEntity] //
                                    , parentDS: Dataset[Row] //
                                    , sanityCount: Long //
                                    , segment: NDataSegment = dataSegment //
                                    , ic: Option[InferiorCountDownLatch] = None)


  private def buildLayout(task: LayoutBuildTask): Unit = {
    val layoutDS = wrapLayoutDS(task.layout, task.parentDS)
    val parentDesc = if (task.parentLayout.isEmpty) {
      if (task.ic.isDefined) "inferior flat table" else "flat table"
    } else task.parentLayout.get.getId
    // set layout mess in build job infos
    val layout: NDataLayout = if (task.parentLayout.isEmpty) null else dataSegment.getLayout(task.parentLayout.get.getId)
    val layoutIdsFromFlatTable = KylinBuildEnv.get().buildJobInfos.getParent2Children.getOrDefault(layout, Sets.newHashSet())
    layoutIdsFromFlatTable.add(task.layout.getId)
    KylinBuildEnv.get().buildJobInfos.recordParent2Children(layout, layoutIdsFromFlatTable)

    val readableDesc = s"Segment $segmentId build layout ${task.layout.getId} from $parentDesc"
    newDataLayout(task.segment, task.layout, layoutDS, readableDesc, Some(new SanityChecker(task.sanityCount)))

    if (task.ic.isDefined) {
      task.ic.get.cdl.countDown()
    }
  }

  protected def tryRefreshColumnBytes(): Unit = {
    if (flatTableStats == null) {
      logInfo(s"Segment $segmentId skip refresh column bytes.")
      return
    }
    logInfo(s"Segment $segmentId refresh column bytes.")
    val stats = flatTableStats
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val dataflowManager = NDataflowManager.getInstance(config, project)
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        copiedSegment.setSourceCount(stats.totalCount)
        // Cal segment dimension range
        if (!jobContext.isPartialBuild) {
          copiedSegment.setDimensionRangeInfoMap(
            calDimRange(dataSegment, flatTable.getFlatTableDS)
          )
        }
        // By design, no fencing.
        val columnBytes = copiedSegment.getColumnSourceBytes
        stats.columnBytes.foreach(kv => columnBytes.put(kv._1, kv._2))
        dataflowUpdate.setToUpdateSegs(copiedSegment)
        // The afterward step would dump the meta to hdfs-store.
        // We should only update the latest meta in mem-store.
        // Make sure the copied dataflow here is the latest.
        dataflowManager.updateDataflow(dataflowUpdate)
      }
    }, project)
  }

  private def needSkipLayout(layout: Long, segment: NDataSegment = dataSegment): Boolean = {
    // Check whether it is resumed or not.
    val dataLayout = segment.getLayout(layout)
    if (Objects.isNull(dataLayout)) {
      return false
    }
    logInfo(s"Segment $segmentId skip layout $layout.")
    true
  }

  protected def tryRefreshBucketMapping(): Unit = {
    val segment = jobContext.getSegment(segmentId)
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val indexPlan = segment.getIndexPlan
        val manager = NIndexPlanManager.getInstance(config, project)
        val mapping = indexPlan.getLayoutBucketNumMapping
        class UpdateBucketMapping extends NIndexPlanUpdater {
          override def modify(copied: IndexPlan): Unit = {
            copied.setLayoutBucketNumMapping(mapping)
          }
        }
        manager.updateIndexPlan(dataflowId, new UpdateBucketMapping)
      }
    }, project)
  }

  // ----------------------------- Beta feature: Inferior Flat Table. ----------------------------- //

  case class InferiorCountDownLatch(inferior: Dataset[Row], cdl: CountDownLatch)

  // Suitable for models generated from multi index recommendations.
  // TODO Make more fantastic abstractions.
  protected def buildInferior(): Unit = {

    val schema = flatTableDS.schema
    val arraySize = schema.size

    val measureMap = dataModel.getEffectiveMeasures.asScala.map { case (id, measure) =>
      val bitmapCol = DictionaryBuilderHelper.needGlobalDict(measure)
      val columns = if (Objects.nonNull(bitmapCol)) {
        val id = dataModel.getColumnIdByColumnName(bitmapCol.getIdentity)
        Seq(s"${id}_KE_ENCODE")
      } else {
        Seq.empty[String]
      } ++
        measure.getFunction.getParameters.asScala.filter(_.isColumnType) //
          .map(p => s"${dataModel.getColumnIdByColumnName(p.getValue)}")
      (id, columns)
    }.toMap

    def getColumns(index: IndexEntity): Seq[String] = {
      val columns = mutable.Set[String]()

      index.getEffectiveDimCols.keySet().asScala.foreach(id => columns.add(s"$id"))

      index.getEffectiveMeasures.keySet().asScala //
        .foreach { measureId =>
          measureMap.getOrElse(measureId, Seq.empty[String]) //
            .foreach(id => columns.add(id))
        }

      columns.toSeq
    }

    def getArray(index: IndexEntity): Array[Double] = {
      val arr = new Array[Double](arraySize)
      getColumns(index).foreach { col =>
        arr(schema.fieldIndex(col)) = 1.0d
      }
      arr
    }

    class ClusterNode(private val node: TreeNode) extends Clusterable {
      override def getPoint: Array[Double] = getArray(node.getIndex)

      def getNode: TreeNode = node
    }

    case class GroupedNodeColumn(nodes: Seq[TreeNode], columns: Seq[String])

    def cluster(k: Int, nodes: Seq[TreeNode]): Seq[GroupedNodeColumn] = {
      val kcluster = new KMeansPlusPlusClusterer[ClusterNode](k, -1, new EarthMoversDistance())
      kcluster.cluster( //
        scala.collection.JavaConverters.seqAsJavaList(nodes.map(n => new ClusterNode(n)))).asScala //
        .map { cluster =>
          val grouped = cluster.getPoints.asScala.map(_.getNode)
          val columns = grouped.map(_.getIndex).flatMap(index => getColumns(index)).distinct.sorted
          GroupedNodeColumn(grouped, columns)
        }
    }

    val (tinyNodes, normalNodes) = spanningTree.getFromFlatTableNodes.asScala.filter(_.nonSpanned())
      // Index comprised of half dimensions should better be built from flat table.
      .filter(node => node.getDimensionSize < arraySize / 2) //
      .partition(node => node.getDimensionSize < 6)

    def getK(nodes: Seq[TreeNode]): Int = {
      val groupFactor = 30
      Math.max(nodes.size / groupFactor //
        , nodes.map(_.getIndex) //
          .flatMap(_.getEffectiveDimCols.keySet().asScala) //
          .distinct.size / groupFactor)
    }

    val tinyClustered = cluster(getK(tinyNodes), tinyNodes)
    val normalClustered = cluster(getK(normalNodes), normalNodes)
    val indexInferiorMap = mutable.HashMap[Long, InferiorCountDownLatch]()
    val inferiors = (tinyClustered ++ normalClustered).map { grouped =>
      val selectColumns = grouped.columns.map(col => expr(col))

      logInfo(s"Segment $segmentId inferior index ${grouped.nodes.map(_.getIndex).map(_.getId).sorted.mkString("[", ",", "]")} " +
        s" columns ${grouped.columns.mkString("[", ",", "]")}")

      val cdl = new CountDownLatch(grouped.nodes.map(node => node.getNonSpannedCount).sum)
      val inferior = flatTableDS.select(selectColumns: _*)
      val ic = InferiorCountDownLatch(inferior, cdl)
      grouped.nodes.foreach(node => indexInferiorMap.put(node.getIndex.getId, ic))
      ic
    }
    cachedIndexInferior = indexInferiorMap.toMap

    def getStorageLevel: StorageLevel = {
      config.getInferiorFlatTableStorageLevel match {
        case "DISK_ONLY_2" => StorageLevel.DISK_ONLY_2
        case "DISK_ONLY_3" => StorageLevel.DISK_ONLY_3
        case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
        case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
        case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
        case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
        case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
        case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
        case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
        case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
        case "OFF_HEAP" => StorageLevel.OFF_HEAP
        case _ => StorageLevel.DISK_ONLY
      }
    }

    val storageLevel = getStorageLevel
    val parallel = inferiors.par
    val forkJoinPool = new ForkJoinPool(inferiors.size)
    try {
      parallel.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(inferiors.size))
      parallel.foreach { ic =>

        logInfo(s"Segment $segmentId inferior persist  ${ic.inferior.columns.mkString("[", ",", "]")}")
        sparkSession.sparkContext.setJobDescription(s"Segment $segmentId inferior persist ${ic.inferior.columns.length}")
        ic.inferior.persist(storageLevel).count()
        sparkSession.sparkContext.setJobDescription(null)
      }
    } finally {
      forkJoinPool.shutdownNow()
    }

    // ---------- un persist -----------
    val delay = 3L
    val unit = TimeUnit.SECONDS

    def unpersist(ic: InferiorCountDownLatch): Unit = {
      if (ic.cdl.getCount > 0) {
        runtime.schedule(() => unpersist(ic), delay, unit)
      } else {
        ic.inferior.unpersist()
        logInfo(s"Segment $segmentId inferior unpersist ${ic.inferior.columns.mkString("[", ",", "]")}")
      }
    }

    inferiors.foreach(inferior => runtime.schedule(() => unpersist(inferior), delay, unit))
    // ---------- un persist -----------
  }

  private def getCachedIndexInferior(index: IndexEntity): Option[InferiorCountDownLatch] = synchronized {
    if (Objects.isNull(cachedIndexInferior)) {
      return None
    }
    val ic = cachedIndexInferior.getOrElse(index.getId, null)
    if (Objects.isNull(ic)) {
      return None
    }
    Some(ic)
  }

  // ----------------------------- Beta feature: Inferior Flat Table. ----------------------------- //

}
