package io.simplesource.shared.topics;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import io.simplesource.kafka.spec.TopicSpec;
//import org.apache.kafka.common.config.TopicConfig;

public class TopicConfigBuilder {
    public final List<String> topicTypes;
    public final Map<String, String> defaultConfigs;
    public final Map<String, Map<String, String>> defaultOverrides;

    private Map<String, TopicSpec> configMap = new HashMap<String, TopicSpec>();
    private Function<String, TopicSpec> defaultSpec = null; // defaultMap(1, 1, 7)
    private TopicNamer topicNamer = name -> name;

    public TopicConfigBuilder(
            List<String> topicTypes,
            Map<String, String> defaultConfigs,
            Map<String, Map<String, String>> defaultOverrides) {
        this.topicTypes = topicTypes;
        this.defaultConfigs = defaultConfigs;
        this.defaultOverrides = defaultOverrides;
    }

    TopicConfigBuilder withConfig(String topicType, TopicSpec tSpec) {
        configMap.put(topicType, tSpec);
        return this;
    }

  TopicConfigBuilder withDefaultConfig(int partitions, int replication, long retentionInDays) {
      defaultSpec = null; //defaultMap(partitions, replication, retentionInDays)
     return this;
  }

    TopicConfig build() {
        
        topicSpecs = topicTypes.<String>stream().map(tt -> {
                    return new Hashtable.Entry<>(tt, configMap.getOrDefault(tt, defaultSpec.apply(tt)));
                }
        ).toMap
    TopicConfig(topicNamer, topicTypes, topicSpecs)
  }

}





//final case class TopicConfigBuilder(topicTypes: List[String],
//                                    defaultConfigs: Map[String, String],
//                                    defaultOverrides: Map[String, Map[String, String]] = Map.empty) {
//
//  val configMap                    = new scala.collection.mutable.HashMap[String, TopicSpec]()
//  var default: String => TopicSpec = defaultMap(1, 1, 7)
//  var topicNamer: TopicNamer       = name => name // identity
//
//  def withTopicNamer(topicNamer: TopicNamer): TopicConfigBuilder = {
//    this.topicNamer = topicNamer
//    this
//  }
//
//  def withConfig(topicType: String, tSpec: TopicSpec): TopicConfigBuilder = {
//    configMap.put(topicType, tSpec)
//    this
//  }
//
//  def withDefaultConfig(partitions: Int, replication: Int, retentionInDays: Long): TopicConfigBuilder = {
//    default = defaultMap(partitions, replication, retentionInDays)
//    this
//  }
//
//  def build(): TopicConfig = {
//    val topicSpecs = topicTypes.map { tt =>
//      (tt, configMap.getOrElse(tt, default(tt)))
//    }.toMap
//    TopicConfig(topicNamer, topicTypes, topicSpecs)
//  }
//
//  private def defaultMap(partitions: Int, replication: Int, retentionInDays: Long)(
//      topicType: String): TopicSpec = {
//    val configMap = defaultOverrides.getOrElse(topicType, defaultConfigs)
//    // configure retention if it is not already set
//    val retentionMap =
//      if (configMap
//            .getOrElse(KafkaTopicConfig.CLEANUP_POLICY_CONFIG, "") == KafkaTopicConfig.CLEANUP_POLICY_COMPACT &&
//          configMap.getOrElse(KafkaTopicConfig.DELETE_RETENTION_MS_CONFIG, "") != "")
//        Map(
//          KafkaTopicConfig.DELETE_RETENTION_MS_CONFIG -> String.valueOf(
//            TimeUnit.DAYS.toMillis(retentionInDays)),
//          KafkaTopicConfig.MIN_COMPACTION_LAG_MS_CONFIG -> String.valueOf(
//            TimeUnit.DAYS.toMillis(retentionInDays))
//        )
//      else
//        Map(
//          KafkaTopicConfig.RETENTION_MS_CONFIG -> String.valueOf(TimeUnit.DAYS.toMillis(retentionInDays))
//        )
//    val usedMap = defaultOverrides.getOrElse(topicType, defaultConfigs) ++ retentionMap
//    new TopicSpec(partitions, replication.toShort, usedMap.asJava)
//  }
//}
//
//object TopicConfigBuilder {
//  type BuildSteps = TopicConfigBuilder => TopicConfigBuilder
//
//  def buildTopics(topicTypes: List[String],
//                  defaultConfigs: Map[String, String],
//                  defaultOverrides: Map[String, Map[String, String]] = Map.empty)(
//      topicBuildFn: TopicConfigBuilder.BuildSteps): TopicConfig = {
//    val topicBuilder = TopicConfigBuilder(topicTypes, defaultConfigs, defaultOverrides)
//    topicBuildFn(topicBuilder)
//    topicBuilder.build()
//  }
//}
