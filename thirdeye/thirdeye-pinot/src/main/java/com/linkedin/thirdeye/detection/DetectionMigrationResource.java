/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import static com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy.*;


/**
 * The Detection migration resource.
 */
@Path("/migrate")
public class DetectionMigrationResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DetectionMigrationResource.class);
  private static final String PROP_WINDOW_DELAY = "windowDelay";
  private static final String PROP_WINDOW_DELAY_UNIT = "windowDelayUnit";
  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final String PROP_WINDOW_UNIT = "windowUnit";

  private final LegacyAnomalyFunctionTranslator translator;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final DetectionConfigManager detectionConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final Yaml yaml;

  /**
   * Instantiates a new Detection migration resource.
   *
   * @param anomalyFunctionFactory the anomaly function factory
   */
  public DetectionMigrationResource(MetricConfigManager metricConfigDAO, AnomalyFunctionManager anomalyFunctionDAO,
      DetectionConfigManager detectionConfigDAO,
      DatasetConfigManager datasetConfigDAO,
      AnomalyFunctionFactory anomalyFunctionFactory,
      AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.detectionConfigDAO = detectionConfigDAO;
    this.datasetConfigDAO = datasetConfigDAO;
    this.translator = new LegacyAnomalyFunctionTranslator(metricConfigDAO, anomalyFunctionFactory, alertFilterFactory);
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    this.yaml = new Yaml(options);
  }

  /**
   * Endpoint to convert a existing anomaly function to a composite pipeline yaml
   *
   * @param anomalyFunctionId the anomaly function id
   * @return the yaml config as string
   */
  @GET
  public String migrateToYaml(@QueryParam("id") long anomalyFunctionId) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    Preconditions.checkArgument(anomalyFunctionDTO.getIsActive(), "try to migrate inactive anomaly function");
    Map<String, Object> yamlConfigs = new LinkedHashMap<>();
    yamlConfigs.put("detectionName", "new_pipeline_" + anomalyFunctionDTO.getFunctionName());
    yamlConfigs.put("metric", anomalyFunctionDTO.getMetric());
    yamlConfigs.put("dataset", anomalyFunctionDTO.getCollection());
    yamlConfigs.put("pipelineType", "Composite");
    if (StringUtils.isNotBlank(anomalyFunctionDTO.getExploreDimensions())) {
      // dimension explore and data filter
      yamlConfigs.put("dimensionExploration",
          getDimensionExplorationParams(anomalyFunctionDTO));
    }
    if (anomalyFunctionDTO.getFilters() != null){
      yamlConfigs.put("filters",
          AnomalyDetectionInputContextBuilder.getFiltersForFunction(anomalyFunctionDTO.getFilters()).asMap());
    }

    Map<String, Object> ruleYaml = new LinkedHashMap<>();

    // detection
    if (anomalyFunctionDTO.getType().equals("WEEK_OVER_WEEK_RULE")){
      // wo1w change detector
      ruleYaml.put("detection", Collections.singletonList(ImmutableMap.of("name", "detection_rule1", "type", "PERCENTAGE_RULE",
          "params", getPercentageChangeRuleDetectorParams(anomalyFunctionDTO))));
    } else if (anomalyFunctionDTO.getType().equals("MIN_MAX_THRESHOLD")){
      // threshold detector
      ruleYaml.put("detection", Collections.singletonList(ImmutableMap.of("name", "detection_rule1", "type", "THRESHOLD",
          "params", getMinMaxThresholdRuleDetectorParams(anomalyFunctionDTO))));
    } else{
      // algorithm detector
      ruleYaml.put("detection", Collections.singletonList(
          ImmutableMap.of("name", "detection_rule1", "type", "ALGORITHM", "params", getAlgorithmDetectorParams(anomalyFunctionDTO),
              PROP_WINDOW_SIZE, anomalyFunctionDTO.getWindowSize(),
              PROP_WINDOW_UNIT, anomalyFunctionDTO.getWindowUnit().toString())));
    }

    // filters
    Map<String, String> alertFilter = anomalyFunctionDTO.getAlertFilter();

    if (alertFilter != null && !alertFilter.isEmpty()){
      Map<String, Object> filterYaml = new LinkedHashMap<>();
      if (!alertFilter.containsKey("thresholdField")) {
        // algorithm alert filter
        filterYaml = ImmutableMap.of("name", "filter_rule1", "type", "ALGORITHM_FILTER", "params", getAlertFilterParams(anomalyFunctionDTO));
      } else {
        // threshold filter migrate to rule filters
        // site wide impact filter migrate to rule based swi filter
        if (anomalyFunctionDTO.getAlertFilter().get("thresholdField").equals("impactToGlobal")){
          filterYaml.put("type", "SITEWIDE_IMPACT_FILTER");
          filterYaml.put("name", "filter_rule1");
          filterYaml.put("params", getSiteWideImpactFilterParams(anomalyFunctionDTO));
        }
        // weight filter migrate to rule based percentage change filter
        if (anomalyFunctionDTO.getAlertFilter().get("thresholdField").equals("weight")){
          filterYaml.put("name", "filter_rule1");
          filterYaml.put("type", "PERCENTAGE_CHANGE_FILTER");
          filterYaml.put("params", getPercentageChangeFilterParams(anomalyFunctionDTO));
        }
      }
      ruleYaml.put("filter", Collections.singletonList(filterYaml));
    }

    yamlConfigs.put("rules", Collections.singletonList(ruleYaml));

    // merger configs
    if (anomalyFunctionDTO.getAnomalyMergeConfig() != null ) {
      Map<String, Object> mergerYaml = new LinkedHashMap<>();
      if (anomalyFunctionDTO.getAnomalyMergeConfig().getMergeStrategy() == FUNCTION_DIMENSIONS){
        mergerYaml.put("maxGap", anomalyFunctionDTO.getAnomalyMergeConfig().getSequentialAllowedGap());
        mergerYaml.put("maxDuration", anomalyFunctionDTO.getAnomalyMergeConfig().getMaxMergeDurationLength());
      }
      yamlConfigs.put("merger", mergerYaml);
    }

    return this.yaml.dump(yamlConfigs);
  }

  private Map<String, Object> getDimensionExplorationParams(AnomalyFunctionDTO functionDTO) throws IOException {
    Map<String, Object> dimensionExploreYaml = new LinkedHashMap<>();
    dimensionExploreYaml.put("dimensions", Collections.singletonList(functionDTO.getExploreDimensions()));
    if (functionDTO.getDataFilter() != null && !functionDTO.getDataFilter().isEmpty() && functionDTO.getDataFilter().get("type").equals("average_threshold")) {
      // migrate average threshold data filter
      dimensionExploreYaml.put("dimensionFilterMetric", functionDTO.getDataFilter().get("metricName"));
      dimensionExploreYaml.put("minValue", Double.valueOf(functionDTO.getDataFilter().get("threshold")));
      dimensionExploreYaml.put("minLiveZone", functionDTO.getDataFilter().get("minLiveZone"));
    }
    if (functionDTO.getType().equals("MIN_MAX_THRESHOLD")){
      // migrate volume threshold
      Properties properties = AnomalyFunctionDTO.toProperties(functionDTO.getProperties());
      if (properties.containsKey("averageVolumeThreshold")){
        dimensionExploreYaml.put("minValue", properties.getProperty("averageVolumeThreshold"));
      }
    }
    return dimensionExploreYaml;
  }

  private Map<String, Object> getPercentageChangeFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    filterYamlParams.put("threshold", Math.abs(Double.valueOf(functionDTO.getAlertFilter().get("maxThreshold"))));
    filterYamlParams.put("pattern", "up_or_down");
    return filterYamlParams;
  }

  private Map<String, Object> getSiteWideImpactFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    filterYamlParams.put("threshold", Math.abs(Double.valueOf(functionDTO.getAlertFilter().get("maxThreshold"))));
    filterYamlParams.put("pattern", "up_or_down");
    filterYamlParams.put("sitewideMetricName", functionDTO.getGlobalMetric());
    filterYamlParams.put("sitewideCollection", functionDTO.getCollection());
    if (StringUtils.isNotBlank(functionDTO.getGlobalMetricFilters())) {
      filterYamlParams.put("filters",
          AnomalyDetectionInputContextBuilder.getFiltersForFunction(functionDTO.getGlobalMetricFilters()).asMap());
    }
    return filterYamlParams;
  }

  private Map<String, Object> getAlertFilterParams(AnomalyFunctionDTO functionDTO) {
    Map<String, Object> filterYamlParams = new LinkedHashMap<>();
    Map<String, Object> params = new HashMap<>();
    filterYamlParams.put("configuration", params);
    params.putAll(functionDTO.getAlertFilter());
    params.put("bucketPeriod", getBucketPeriod(functionDTO));
    params.put("timeZone", getTimezone(functionDTO));
    return filterYamlParams;
  }

  private String getTimezone(AnomalyFunctionDTO functionDTO) {
    DatasetConfigDTO datasetConfigDTO = this.datasetConfigDAO.findByDataset(functionDTO.getCollection());
    return datasetConfigDTO.getTimezone();
  }

  private String getBucketPeriod(AnomalyFunctionDTO functionDTO) {
    return new Period(TimeUnit.MILLISECONDS.convert(functionDTO.getBucketSize(), functionDTO.getBucketUnit())).toString();
  }

  private Map<String, Object> getPercentageChangeRuleDetectorParams(AnomalyFunctionDTO functionDTO) throws IOException {
    Map<String, Object> detectorYaml = new LinkedHashMap<>();
    Properties properties = AnomalyFunctionDTO.toProperties(functionDTO.getProperties());
    double threshold = Double.valueOf(properties.getProperty("changeThreshold"));
    if (properties.containsKey("changeThreshold")){
      detectorYaml.put("percentageChange", Math.abs(threshold));
      if (threshold > 0){
        detectorYaml.put("pattern", "UP");
      } else {
        detectorYaml.put("pattern", "DOWN");
      }
    }
    return detectorYaml;
  }

  private Map<String, Object> getMinMaxThresholdRuleDetectorParams(AnomalyFunctionDTO functionDTO) throws IOException {
    Map<String, Object> detectorYaml = new LinkedHashMap<>();
    Properties properties = AnomalyFunctionDTO.toProperties(functionDTO.getProperties());
    if (properties.containsKey("min")){
      detectorYaml.put("min", properties.getProperty("min"));
    }
    if (properties.containsKey("max")){
      detectorYaml.put("max", properties.getProperty("max"));
    }
    return detectorYaml;
  }

  private Map<String, Object> getAlgorithmDetectorParams(AnomalyFunctionDTO functionDTO) throws Exception {
    Map<String, Object> detectorYaml = new LinkedHashMap<>();
    Map<String, Object> params = new LinkedHashMap<>();
    detectorYaml.put("configuration", params);
    Properties properties = AnomalyFunctionDTO.toProperties(functionDTO.getProperties());
    for (Map.Entry<Object, Object> property : properties.entrySet()) {
      params.put((String) property.getKey(), property.getValue());
    }
    params.put("variables.bucketPeriod", getBucketPeriod(functionDTO));
    params.put("variables.timeZone", getTimezone(functionDTO));
    if (functionDTO.getWindowDelay() != 0) {
      detectorYaml.put(PROP_WINDOW_DELAY, functionDTO.getWindowDelay());
      detectorYaml.put(PROP_WINDOW_DELAY_UNIT, functionDTO.getWindowDelayUnit().toString());
    }
    return detectorYaml;
  }

  /**
   * This endpoint takes in a anomaly function Id and translate the anomaly function config to a
   * detection config of the new pipeline and then persist it in to database.
   *
   * @param anomalyFunctionId the anomaly function id
   * @return the response
   * @throws Exception the exception
   */
  @POST
  public Response migrateToDetectionPipeline(@QueryParam("id") long anomalyFunctionId, @QueryParam("name") String name,
      @QueryParam("lastTimestamp") Long lastTimestamp) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = this.anomalyFunctionDAO.findById(anomalyFunctionId);
    DetectionConfigDTO config = this.translator.translate(anomalyFunctionDTO);

    if (!StringUtils.isBlank(name)) {
      config.setName(name);
    }

    config.setLastTimestamp(System.currentTimeMillis());
    if (lastTimestamp != null) {
      config.setLastTimestamp(lastTimestamp);
    }

    this.detectionConfigDAO.save(config);
    if (config.getId() == null) {
      throw new WebApplicationException(String.format("Could not migrate anomaly function %d", anomalyFunctionId));
    }

    LOGGER.info("Created detection config {} for anomaly function {}", config.getId(), anomalyFunctionDTO.getId());
    return Response.ok(config.getId()).build();
  }
}
