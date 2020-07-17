package org.weiwan.argus.core.flink.pub

import java.io.IOException

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.weiwan.argus.core.flink.utils.FlinkContextUtil

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/29 10:25
 * @Package: com.hopson.dc.realtime.java.init
 * @ClassName: package
 * @Description:
 **/
class ScalaEnvIniter extends BaseEnvIniter with EnvIniter[FlinkContext[ExecutionEnvironment], FlinkContext[StreamExecutionEnvironment]] {

  val logger: FlinkLogger = FlinkContextUtil.getLogger(getClass)

  override def initBatch(context: FlinkContext[ExecutionEnvironment]): Unit = {
    logger.info("initialize flinkContext for Batch-Env!")
    val confFilePath = getConfFilePath(context, isLocalBatch(context))
    val mergeTool = mergeEnvConfig(context, confFilePath)
    //最终配置放入到env环境中
    context.getEnv.getConfig.setGlobalJobParameters(mergeTool)
    //初始化通用配置
    initFlinkCommon(context);
    //初始化批参数
    initFlinkBatch(context)
  }

  override def initStream(context: FlinkContext[StreamExecutionEnvironment]): Unit = {
    logger.info("initialize flinkContext for Stream-Env")

    //1. 初始化Flink配置
    val confFilePath = getConfFilePath(context, isLocalStream(context));
    val mergeTool = mergeEnvConfig(context, confFilePath);
    context.getEnv.getConfig.setGlobalJobParameters(mergeTool)
    logger.debug("read flinkContext configuration information completed")

    //2. 初始化Flink-Common配置
    initFlinkCommon(context)
    //3. 初始化Flink-Stream配置
    initFlinkStream(context)
    //4. 初始化检查点配置
    initCheckpoint(context)
    logger.debug("initializes a checkpoint for the flinkContext of the stream-Env")
    //5. 初始化状态后端
    initStateBackend(context)
    logger.debug("initializes a stateBackend for the flinkContext of the stream-Env")
    logger.info("the flinkContext initialization of Stream-Env is completed")

  }

  private def initStateBackend(context: FlinkContext[StreamExecutionEnvironment]): Unit = {
    val env = context.getEnv
    val flinkConfig = context.getFlinkConfig
    if (isLocalStream(context)) { //本地的话 使用内存后端就可以了
      val stateBackend = new MemoryStateBackend
      env.setStateBackend(stateBackend)
    }
    try //如果配置了其它后端,使用配置的,本地如果不想使用自定义状态后端,可以在配置文件中注释掉
      env.setStateBackend(useStateBackend(flinkConfig))
    catch {
      case e: IOException =>
        e.printStackTrace()
        logger.error("init state backend failed, please check the config file !", e)
    }
  }


  @throws[IOException]
  private def initCheckpoint(context: FlinkContext[StreamExecutionEnvironment]): Unit = {
    val env = context.getEnv
    val flinkConfig = context.getFlinkConfig
    val checkPointEnable = flinkConfig.getVar(FlinkContains.FLINK_TASK_CHECKPOINT_ENABLE_KEY)
    if (!StringUtils.isEmpty(checkPointEnable) && checkPointEnable.toBoolean) { //默认值设置
      checkPointDefault(env)
      //启用checkpoint
      val checkPointInterval = flinkConfig.getVar(FlinkContains.FLINK_TASK_CHECKPOINT_INTERVAL_KEY)
      val checkPointMode = flinkConfig.getVar(FlinkContains.FLINK_TASK_CHECKPOINT_MODE_KEY)
      val notEmpty = StringUtils.isNoneEmpty(checkPointEnable,checkPointInterval, checkPointMode)
      if (notEmpty) {
        env.enableCheckpointing(checkPointInterval.toLong, CheckpointingMode.valueOf(checkPointMode))
        val point = env.getCheckpointConfig
        configureCheckPoint(flinkConfig, point)
      }
    } else {
      logger.debug("not enable checkpoint, skip init checkpoint!")
    }
  }

  /**
   * 设置checkpoint默认值
   *
   * @param env
   */
  private def checkPointDefault(env: StreamExecutionEnvironment): Unit = {
    env.enableCheckpointing(FlinkDefault.CHECKPOINT_INTERVAL_DEFAULT, FlinkDefault.CHECKPOINT_MODE)
    val point = env.getCheckpointConfig
    point.setCheckpointTimeout(FlinkDefault.CHECKPOINT_TIMEOUT_DEFAULT.toLong)
    point.setMinPauseBetweenCheckpoints(FlinkDefault.CHECKPOINT_MIN_PAUSE_BETWEEN_DEFAULT.toLong)
    point.setMaxConcurrentCheckpoints(FlinkDefault.CHECKPOINT_MAX_CONCURRENT_DEFAULT.toInt)
    point.setFailOnCheckpointingErrors(FlinkDefault.ON_FAIL_DEFAULT.toBoolean)
    if (FlinkDefault.EXTERNALIZED_ENABLE_DEFAULT.toBoolean)
      point.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
  }

  override def isLocalStream(k: FlinkContext[StreamExecutionEnvironment]): Boolean = {
    k.getEnv.isInstanceOf[StreamExecutionEnvironment]
  }

  override def isLocalBatch(v: FlinkContext[ExecutionEnvironment]): Boolean = {
    v.getEnv.isInstanceOf[ExecutionEnvironment]
  }


  private def initFlinkStream(context: FlinkContext[StreamExecutionEnvironment]): Unit = {
    val flinkConfig = context.getFlinkConfig
    val env = context.getEnv
    val orDefault = flinkConfig.getOrDefault(FlinkContains.FLINK_TASK_STREAM_TIME_CHARACTERISTIC_KEY, FlinkDefault.FLINK_TASK_STREAM_TIME_CHARACTERISTIC_KEY)
    env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(orDefault))
  }

  private def initFlinkBatch(context: FlinkContext[ExecutionEnvironment]): Unit = {
    val env = context.getEnv
    //        env.setSessionTimeout(1);
  }

  private def initFlinkCommon(context: FlinkContext[_]): Unit = {
    val conf = context.getFlinkConfig
    val parallelism = conf.getOrDefault(FlinkContains.FLINK_TASK_COMMON_PARALLELISM_KEY, FlinkDefault.FLINK_TASK_COMMON_PARALLELISM_DEFAULT)
    if (isStream(context)) {
      val env = context.getEnv.asInstanceOf[StreamExecutionEnvironment]
      //设置并行度
      env.setParallelism(parallelism.toInt)
      //选择重启策略
      env.setRestartStrategy(chooseARestartStrategy(context))
    }
    if (isBatch(context)) {
      val env = context.getEnv.asInstanceOf[ExecutionEnvironment]
      env.setParallelism(Integer.valueOf(parallelism))
      env.setRestartStrategy(chooseARestartStrategy(context))
    }
  }

}
