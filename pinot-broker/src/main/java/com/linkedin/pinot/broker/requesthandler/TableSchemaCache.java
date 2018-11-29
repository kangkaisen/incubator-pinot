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
package com.linkedin.pinot.broker.requesthandler;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableSchemaCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaCache.class);

  private final LoadingCache<String, Schema> _tableSchemaCache;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ExecutorService _executorService;

  public TableSchemaCache(ZkHelixPropertyStore<ZNRecord> propertyStore, int maxCacheSize, int cacheTimeoutInMinute) {
    LOGGER.info("Init table schema cache. maxCacheSize: {}, cacheTimeoutInMinute: {}", maxCacheSize,
        cacheTimeoutInMinute);
    _executorService = Executors.newCachedThreadPool();
    _propertyStore = propertyStore;
    _tableSchemaCache = CacheBuilder.newBuilder()
        .maximumSize(maxCacheSize)
        .refreshAfterWrite(cacheTimeoutInMinute, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Schema>() {
          @Override
          public Schema load(@Nonnull String rawTableName) {
            return ZKMetadataProvider.getTableSchema(_propertyStore, rawTableName);
          }

          @Override
          public ListenableFuture<Schema> reload(String key, Schema oldValue) {
            ListenableFutureTask<Schema> task =
                ListenableFutureTask.create(() -> ZKMetadataProvider.getTableSchema(_propertyStore, key));
            _executorService.execute(task);
            return task;
          }
        });
  }

  /**
   * Gets table schema if it's present.
   * @param tableName Table name with or without type suffix.
   */
  public Schema getIfTableSchemaPresent(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    return _tableSchemaCache.getIfPresent(rawTableName);
  }

  /**
   * Refreshes table schema.
   * @param tableName Table name with or without type suffix.
   */
  public void refreshTableSchema(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    _tableSchemaCache.refresh(rawTableName);
  }
}
