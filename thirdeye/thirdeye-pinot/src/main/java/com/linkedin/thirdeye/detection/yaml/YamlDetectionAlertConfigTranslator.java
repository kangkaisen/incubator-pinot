package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import com.linkedin.thirdeye.detection.annotation.registry.DetectionRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;


/**
 * The translator converts the alert yaml config into a detection alert config
 */
public class YamlDetectionAlertConfigTranslator {
  public static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  public static final String PROP_RECIPIENTS = "recipients";

  static final String PROP_SUBS_GROUP_NAME = "subscriptionGroupName";
  static final String PROP_CRON = "cron";
  static final String PROP_ACTIVE = "active";
  static final String PROP_APPLICATION = "application";
  static final String PROP_FROM = "fromAddress";
  static final String PROP_ONLY_FETCH_LEGACY_ANOMALIES = "onlyFetchLegacyAnomalies";
  static final String PROP_EMAIL_SUBJECT_TYPE = "emailSubjectStyle";
  static final String PROP_ALERT_SCHEMES = "alertSchemes";
  static final String PROP_ALERT_SUPPRESSORS = "alertSuppressors";
  static final String PROP_REFERENCE_LINKS = "referenceLinks";

  static final String PROP_TYPE = "type";
  static final String PROP_CLASS_NAME = "className";
  static final String PROP_PARAM = "params";

  static final String PROP_DIMENSION = "dimension";
  static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";
  static final String PROP_TIME_WINDOWS = "timeWindows";
  static final String CRON_SCHEDULE_DEFAULT = "0 0/5 * * * ? *"; // Every 5 min

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  private static final DetectionAlertRegistry DETECTION_ALERT_REGISTRY = DetectionAlertRegistry.getInstance();
  private static final Set<String> PROPERTY_KEYS = new HashSet<>(
      Arrays.asList(PROP_DETECTION_CONFIG_IDS, PROP_RECIPIENTS, PROP_DIMENSION, PROP_DIMENSION_RECIPIENTS));

  private static final YamlDetectionAlertConfigTranslator INSTANCE = new YamlDetectionAlertConfigTranslator();

  public static YamlDetectionAlertConfigTranslator getInstance() {
    return INSTANCE;
  }

  /**
   * generate detection alerter from YAML
   * @param alertYamlConfigs yaml configuration of the alerter
   * @param detectionConfigIds detection config ids that should be included in the detection alerter
   * @param existingVectorClocks vector clocks that should be kept in the new alerter
   * @return a detection alert config
   */
  public DetectionAlertConfigDTO generateDetectionAlertConfig(Map<String, Object> alertYamlConfigs,
      Collection<Long> detectionConfigIds, Map<Long, Long> existingVectorClocks) {
    DetectionAlertConfigDTO alertConfigDTO = new DetectionAlertConfigDTO();
    Preconditions.checkArgument(alertYamlConfigs.containsKey(PROP_SUBS_GROUP_NAME), "Alert property missing: " + PROP_SUBS_GROUP_NAME);

    if (existingVectorClocks == null) {
      existingVectorClocks = new HashMap<>();
    }
    for (long detectionConfigId : detectionConfigIds) {
      if (!existingVectorClocks.containsKey(detectionConfigId)){
        existingVectorClocks.put(detectionConfigId, 0L);
      }
    }
    alertConfigDTO.setVectorClocks(existingVectorClocks);

    alertConfigDTO.setName(MapUtils.getString(alertYamlConfigs, PROP_SUBS_GROUP_NAME));
    alertConfigDTO.setCronExpression(MapUtils.getString(alertYamlConfigs, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    alertConfigDTO.setActive(true);
    alertConfigDTO.setApplication(MapUtils.getString(alertYamlConfigs, PROP_APPLICATION));
    alertConfigDTO.setProperties(buildAlerterProperties(alertYamlConfigs, detectionConfigIds));
    return alertConfigDTO;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs, Collection<Long> detectionConfigIds) {
    Map<String, Object> properties = buildAlerterProperties(alertYamlConfigs);
    properties.put(PROP_DETECTION_CONFIG_IDS, detectionConfigIds);
    return properties;
  }

  private Map<String, Object> buildAlerterProperties(Map<String, Object> alertYamlConfigs) {
    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<String, Object> entry : alertYamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, DETECTION_REGISTRY.lookup(MapUtils.getString(alertYamlConfigs, PROP_TYPE)));
      } else {
        if (PROPERTY_KEYS.contains(entry.getKey())) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return properties;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>> buildAlertSuppressors(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSuppressors = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SUPPRESSORS));
    Map<String, Map<String, Object>> alertSuppressorsHolder = new HashMap<>();
    Map<String, Object> alertSuppressorsParsed = new HashMap<>();
    if (!alertSuppressors.isEmpty()) {
      for (Map<String, Object> alertSuppressor : alertSuppressors) {
        Map<String, Object> alertSuppressorsTimeWindow = new HashMap<>();
        if (alertSuppressor.get(PROP_TYPE) != null) {
          alertSuppressorsTimeWindow.put(PROP_CLASS_NAME,
              DETECTION_ALERT_REGISTRY.lookupAlertSuppressors(alertSuppressor.get(PROP_TYPE).toString()));
        }

        if (alertSuppressor.get(PROP_PARAM) != null) {
          for (Map.Entry<String, Object> params : ((Map<String, Object>) alertSuppressor.get(PROP_PARAM)).entrySet()) {
            alertSuppressorsParsed.put(params.getKey(), params.getValue());
          }
        }

        String suppressorType =
            CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, alertSuppressor.get(PROP_TYPE).toString());
        alertSuppressorsTimeWindow.put(PROP_TIME_WINDOWS, new ArrayList<>(Arrays.asList(alertSuppressorsParsed)));
        alertSuppressorsHolder.put(suppressorType + "Suppressor", alertSuppressorsTimeWindow);
      }
    }

    return alertSuppressorsHolder;
  }

  @SuppressWarnings("unchecked")
  private Map<String,Map<String,Object>>  buildAlertSchemes(Map<String,Object> yamlAlertConfig) {
    List<Map<String, Object>> alertSchemes = ConfigUtils.getList(yamlAlertConfig.get(PROP_ALERT_SCHEMES));
    Map<String, Map<String, Object>> alertSchemesHolder = new HashMap<>();
    Map<String, Object> alertSchemesParsed = new HashMap<>();
    if (!alertSchemes.isEmpty()) {
      for (Map<String, Object> alertScheme : alertSchemes) {
        if (alertScheme.get(PROP_TYPE) != null) {
          alertSchemesParsed.put(PROP_CLASS_NAME,
              DETECTION_ALERT_REGISTRY.lookupAlertSchemes(alertScheme.get(PROP_TYPE).toString()));
        }

        if (alertScheme.get(PROP_PARAM) != null) {
          for (Map.Entry<String, Object> params : ((Map<String, Object>) alertScheme.get(PROP_PARAM)).entrySet()) {
            alertSchemesParsed.put(params.getKey(), params.getValue());
          }
        }

        alertSchemesHolder.put(alertScheme.get(PROP_TYPE).toString().toLowerCase() + "Scheme", alertSchemesParsed);
      }
    }

    return alertSchemesHolder;
  }

  /**
   * Generates the {@link DetectionAlertConfigDTO} from the YAML Alert Map
   */
  @SuppressWarnings("unchecked")
  public DetectionAlertConfigDTO translate(Map<String,Object> yamlAlertConfig) {
    DetectionAlertConfigDTO alertConfigDTO = new DetectionAlertConfigDTO();

    alertConfigDTO.setName(MapUtils.getString(yamlAlertConfig, PROP_SUBS_GROUP_NAME));
    alertConfigDTO.setApplication(MapUtils.getString(yamlAlertConfig, PROP_APPLICATION));
    alertConfigDTO.setFrom(MapUtils.getString(yamlAlertConfig, PROP_FROM));

    alertConfigDTO.setCronExpression(MapUtils.getString(yamlAlertConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    alertConfigDTO.setActive(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ACTIVE, true));
    alertConfigDTO.setOnlyFetchLegacyAnomalies(MapUtils.getBooleanValue(yamlAlertConfig, PROP_ONLY_FETCH_LEGACY_ANOMALIES, false));
    alertConfigDTO.setSubjectType((AlertConfigBean.SubjectType) MapUtils.getObject(yamlAlertConfig, PROP_EMAIL_SUBJECT_TYPE, AlertConfigBean.SubjectType.METRICS));

    Map<String, String> refLinks = MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS);
    if (refLinks == null) {
      refLinks = new HashMap<>();
      yamlAlertConfig.put(PROP_REFERENCE_LINKS, refLinks);
    }
    refLinks.put("ThirdEye User Guide", "https://go/thirdeyeuserguide");
    refLinks.put("Add Reference Links", "https://go/thirdeyealertreflink");
    alertConfigDTO.setReferenceLinks(MapUtils.getMap(yamlAlertConfig, PROP_REFERENCE_LINKS));

    alertConfigDTO.setAlertSchemes(buildAlertSchemes(yamlAlertConfig));
    alertConfigDTO.setAlertSuppressors(buildAlertSuppressors(yamlAlertConfig));
    alertConfigDTO.setProperties(buildAlerterProperties(yamlAlertConfig));

    // NOTE: The below fields will/should be hidden from the YAML/UI. They will only be updated by the backend pipeline.
    List<Integer> detectionConfigIds = ConfigUtils.getList(yamlAlertConfig.get(PROP_DETECTION_CONFIG_IDS));
    Map<Long, Long> vectorClocks = new HashMap<>();
    for (int detectionConfigId : detectionConfigIds) {
      vectorClocks.put((long) detectionConfigId, 0L);
    }
    alertConfigDTO.setHighWaterMark(0L);
    alertConfigDTO.setVectorClocks(vectorClocks);

    return alertConfigDTO;
  }
}
