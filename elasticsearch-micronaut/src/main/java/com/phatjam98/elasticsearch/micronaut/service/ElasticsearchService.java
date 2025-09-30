package com.phatjam98.elasticsearch.micronaut.service;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.ReindexRequest;
import co.elastic.clients.elasticsearch.core.ReindexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.elasticsearch.indices.CloneIndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.FreezeResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.elasticsearch.indices.UnfreezeResponse;
import co.elastic.clients.elasticsearch.indices.UpdateAliasesResponse;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.elasticsearch.indices.update_aliases.ActionVariant;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.phatjam98.elasticsearch.utils.IndexUtils;
import com.phatjam98.elasticsearch.utils.MappingsComparator;
import io.micronaut.core.io.ResourceResolver;
import io.micronaut.core.io.scan.ClassPathResourceLoader;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.elasticsearch.ExceptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This service interacts with Elasticsearch.
 */
@Singleton
public class ElasticsearchService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchService.class);

  @Inject
  ElasticsearchAsyncClient esAsyncClient;

  private final int shards;
  private final int replicas;

  /**
   * Set replicas and shards based on the Elasticsearch {@link HealthResponse}.  Set replicas
   * to 1 less than total number of Data Nodes.
   *
   * @param esAsyncClient ElasticsearchAsyncClient
   *                      {@link ElasticsearchAsyncClient}
   */
  public ElasticsearchService(ElasticsearchAsyncClient esAsyncClient) {
    this.esAsyncClient = esAsyncClient;
    HealthResponse cluster = clusterHealth();

    this.replicas = Math.max(cluster.numberOfDataNodes() - 1, 0);
    // For now we will strictly set shards at 3.  Generally we want 1 shard per 10GB of data.
    // At this time we are nowhere near that.  We will revisit this in the future.
    this.shards = 3;
  }

  /**
   * Used to check if an index exists or not.
   *
   * @param index String name of the index to check.
   * @return Boolean
   */
  public Boolean indexExists(String index) {
    BooleanResponse booleanResponse = null;
    var cf = esAsyncClient.indices().exists(
        new ExistsRequest.Builder().index(index).build()
    ).whenComplete((response, exception) -> {
      if (exception != null) {
        LOGGER.error("Exception while checking if the index {} exists", index, exception);
      } else {
        LOGGER.info("Index {} exists: {}", index, response.value());
      }
    });

    try {
      booleanResponse = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while checking if the index {} exists", index, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Execution exception while checking if the index {} exists", index, e);
    }

    return booleanResponse != null && booleanResponse.value();
  }

  /**
   * Create an index with the index name.
   *
   * @param indexName String name of the index to create
   * @param mappings  Map mappings generated from resource
   * @return CreateIndexResponse
   */
  public CreateIndexResponse createIndex(String indexName, TypeMapping mappings) {
    CreateIndexResponse response = null;

    var cf = esAsyncClient.indices().create(
        new CreateIndexRequest.Builder().index(indexName)
            .settings(s -> s.numberOfShards(Integer.toString(shards))
                .numberOfReplicas(Integer.toString(replicas))).mappings(mappings).build()
    ).whenComplete((resp, exception) -> {
      if (exception != null) {
        LOGGER.error("Exception while creating the index {}", indexName, exception);
      } else {
        LOGGER.info("Index {} created: {}", indexName, resp.acknowledged());
      }
    });

    try {
      response = cf.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return response;
  }

  /**
   * Deletes the given index use with care.
   *
   * @param indexName String index name to delete
   * @return DeleteIndexResponse
   * @see DeleteIndexResponse
   */
  public DeleteIndexResponse deleteIndex(String indexName) {
    var cf = esAsyncClient.indices().delete(r -> r.index(indexName))
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while deleting the index {}", indexName, exception);
          } else {
            LOGGER.info("Index {} deleted: {}", indexName, resp.acknowledged());
          }
        });

    DeleteIndexResponse response = null;

    try {
      response = cf.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return response;
  }

  /**
   * Creates a new document in the given index.
   *
   * @param indexName String name of the index to create the document in.
   * @param docId     String id of the document in elasticsearch.
   *                  This should be the same as the documents ID field.
   * @param jsonDoc   String json string representation of the document.
   * @return CreateResponse
   */
  public CreateResponse create(String indexName, String docId, String jsonDoc) {
    var cf = esAsyncClient.create(
        new CreateRequest.Builder<>().index(indexName).id(docId)
            .withJson(new StringReader(jsonDoc)).build()
    ).whenComplete((response, exception) -> {
      if (exception != null) {
        LOGGER.error("Exception while creating the document. index: {}, doc: {}", indexName,
            jsonDoc, exception);
      } else {
        LOGGER.info("Document created. index: {}, doc: {}", indexName, jsonDoc);
      }
    });

    CreateResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to create a document. index: {}, doc: {}",
          indexName, jsonDoc, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Execution exception while trying to create a document. index: {}, doc: {}",
          indexName, jsonDoc, e);
    }

    return response;
  }

  /**
   * Update a document in Elasticsearch.
   *
   * @param indexName String name of the index
   * @param docId     String Elasticsearch document ID
   * @param docMap    Map json document
   * @return UpdateResponse indicating update result
   */

  public UpdateResponse<Map> update(String indexName, String docId, Map<String, Object> docMap) {
    var cf = esAsyncClient.update(ur -> ur.index(indexName).id(docId).doc(docMap), Map.class)
        .whenComplete((response, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while updating the document. index: {}, doc: {}", indexName,
                docMap, exception);
          } else {
            LOGGER.info("Document updated. index: {}, doc: {}", indexName, docMap);
          }
        });

    UpdateResponse<Map> response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to update a document. index: {}, doc: {}",
          indexName, docMap, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Execution exception while trying to update a document. index: {}, doc: {}",
          indexName, docMap, e);
    }

    return response;
  }

  /**
   * Executes {@link SearchRequest} and returns {@link SearchResponse}.  The SearchResponse contains
   * the Hits.  Hits are documents returned by the search.
   *
   * @param searchRequest SearchRequest The prepared SearchRequest.
   * @param <T>           Class of the Protobuf resource.
   * @return SearchResponse Hits will be returned.
   */
  public <T> SearchResponse<T> search(SearchRequest searchRequest, Class<T> klass) {
    var cf = esAsyncClient.search(searchRequest, klass)
        .whenComplete((response, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while searching with request: {}", searchRequest, exception);
          } else {
            LOGGER.info("Search completed with request: {}", searchRequest);
          }
        });

    SearchResponse<T> response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to search with request: {}", searchRequest,
          e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Execution exception while trying to search with request: {}", searchRequest, e);
    }

    return response;
  }

  /**
   * Forces the index to refresh.  Elasticsearch refreshes once every second for any index that has
   * had a query in the last 30 seconds.  Refreshing is resource intensive and should only be used
   * when necessary.
   *
   * @param request RefreshRequest
   * @return Refresh Response
   */
  public RefreshResponse refresh(RefreshRequest request) {
    var cf = esAsyncClient.indices().refresh(request).whenComplete((resp, exception) -> {
      if (exception != null) {
        LOGGER.error("Exception while trying to refresh the index: {}", request, exception);
      } else {
        LOGGER.info("Index refreshed: {}", request);
      }
    });

    RefreshResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to refresh the index: {}", request,
          e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Execution exception while trying to refresh the index: {}", request, e);
    }

    return response;
  }

  /**
   * This is used to initialize any index that will be used by a service.  Pass in a list of
   * Protobuf Resource Classes on start up. Flow of this is as follows:
   * <ol>
   *   <li>Get indexName, alias and mappings from the protobuf resource. For example:
   *     <ul>
   *       <li>indexName = risk_score-1234</li>
   *       <li>alias = risk_score</li>
   *       <li>mappings = Map</li>
   *     </ul>
   *   </li>
   *   <li>Check if index exists by alias.  If not creates the index with the provided mappings and
   *   creates the alias.</li>
   *   <li>If the index exists, Mappings are checked between the new and the existing.  If there is
   *   a mismatch {@link ElasticsearchService#updateMappings(Class)}
   *   is called to try and update mappings.</li>
   *   <li>If {@link ElasticsearchService#updateMappings(Class)} is
   *   unsuccessful, or a post update call to
   *   {@link ElasticsearchService#isMatchMappings(Class)} returns
   *   FALSE {@link ElasticsearchService#reindex(Class)} is called to
   *   reindex from old to new index.  If that succeeds the Alias is swapped by calling
   *   {@link ElasticsearchService#swapAlias(Class)} and Mappings are
   *   checked once again with
   *   {@link ElasticsearchService#isMatchMappings(Class)}</li>
   *   <li>If all of this fails we log the error that Mappings failed to update for the given
   *   index, and we throw a RuntimeException causing the application to terminate.</li>
   * </ol>
   *
   * @param resources List of Protobuf Classes used to check and create indices
   */
  @SafeVarargs
  public final void bootstrapService(Class<? extends GeneratedMessageV3>... resources)
      throws ElasticsearchException {
    for (Class<? extends GeneratedMessageV3> resource : resources) {
      var indexName = IndexUtils.getIndexName(resource);
      var alias = IndexUtils.getAlias(resource);
      var mappings = IndexUtils.getTypeMapping(resource);

      if (Boolean.FALSE.equals(indexExists(alias))) {
        createIndex(indexName, mappings);
        LOGGER.info("Index created for {}", indexName);
        LOGGER.info("Mappings for index {} set as: {}", indexName, mappings);
        updateAliases(indexName, alias, Action.Kind.Add);
      } else if (Boolean.TRUE.equals(isMatchMappings(resource))) {
        LOGGER.info("Index exists for {}, and mappings match.", indexName);
      } else if (Boolean.TRUE.equals(updateMappings(resource))
          && Boolean.TRUE.equals(isMatchMappings(resource))) {
        LOGGER.info("Mappings were successfully updated for {}", indexName);
      } else if (Boolean.TRUE.equals(reindex(resource)) && Boolean.TRUE.equals(swapAlias(resource))
          && Boolean.TRUE.equals(isMatchMappings(resource))) {
        LOGGER.info("Index reindexed and Mappings match for {}", indexName);
      } else {
        LOGGER.error("Mappings failed to update for {}", indexName);
        throw ExceptionsHelper.convertToElastic(
            new RuntimeException("We need to know immediately if mappings updates fail."));
      }
    }
  }

  private Boolean swapAlias(Class<? extends GeneratedMessageV3> resource) {
    var alias = IndexUtils.getAlias(resource);
    var indexName = IndexUtils.getIndexName(resource);
    var oldIndexNames = indexNamesFromAlias(alias);
    UpdateAliasesResponse addAliasResponse =
        updateAliases(indexName, alias, Action.Kind.Add);
    UpdateAliasesResponse removeAliasResponse = null;

    if (addAliasResponse != null && addAliasResponse.acknowledged()) {
      removeAliasResponse = updateAliases(oldIndexNames, alias, Action.Kind.Remove);
    }

    return addAliasResponse != null && removeAliasResponse != null
        && removeAliasResponse.acknowledged();
  }

  /**
   * Clones one index to a new index.  This is used in migrating from a legacy index to new or some
   * other heavy lift needed to move from one version of an index to another.
   *
   * @param targetIndexName String indexName to clone old into
   * @param sourceIndexName String indexName to clone from
   * @return CloneIndexResponse
   */
  public CloneIndexResponse cloneIndex(String targetIndexName, String sourceIndexName) {
    var cf = esAsyncClient.indices().clone(r -> r.index(sourceIndexName).target(targetIndexName)
            .timeout(t -> t.time("10m")))
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to clone an Index. sourceIndex: {}, "
                + "targetIndex: {}", sourceIndexName, targetIndexName, exception);
          } else {
            LOGGER.info("Index cloned. sourceIndex: {}, targetIndex: {}", sourceIndexName,
                targetIndexName);
          }
        });

    CloneIndexResponse response = null;

    if (Boolean.TRUE.equals(freezeIndex(sourceIndexName).acknowledged())) {
      try {
        response = cf.get();
      } catch (InterruptedException e) {
        LOGGER.error("Thread was interrupted while trying to clone an Index. sourceIndex: {}, "
            + "targetIndex: {}", sourceIndexName, targetIndexName, e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        throw ExceptionsHelper.convertToElastic(e);
      }
    } else {
      LOGGER.error("Failed to freeze the index to clone. indexName: {}", sourceIndexName);
    }

    return response;
  }

  private FreezeResponse freezeIndex(String sourceIndexName) {
    var cf =
        esAsyncClient.indices().freeze(r -> r.index(sourceIndexName).timeout(t -> t.time("2m")))
            .whenComplete((resp, exception) -> {
              if (exception != null) {
                LOGGER.error("Exception while trying to freeze an Index. index: {}",
                    sourceIndexName, exception);
              } else {
                LOGGER.info("Index frozen. index: {}", sourceIndexName);
              }
            });

    FreezeResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to freeze Index. index: {}",
          sourceIndexName, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    return response;
  }

  private UnfreezeResponse unFreezeIndex(String indexName) {
    var cf = esAsyncClient.indices()
        .unfreeze(r -> r.index(indexName).timeout(t -> t.time("1m")))
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to unFreeze an Index. index: {}",
                indexName, exception);
          } else {
            LOGGER.info("Index unfrozen. index: {}", indexName);
          }
        });

    UnfreezeResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to unFreeze Index. index: {}",
          indexName, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    return response;
  }

  /**
   * Reindex creates a new Index from the latest Mappings and reindexes the existing index into
   * the new.  This is different than clone as this will attempt to write the current docs into the
   * new Mappings.
   *
   * @param resource GeneratedMessageV3 proto resource
   * @return Boolean success or failure
   */
  public ReindexResponse reindex(Class<? extends GeneratedMessageV3> resource) {
    var newIndexName = IndexUtils.getIndexName(resource);
    var sourceAlias = IndexUtils.getAlias(resource);

    if (Boolean.TRUE.equals(indexExists(newIndexName))) {
      prepareReindex(newIndexName, sourceAlias);
    }

    createIndex(newIndexName, IndexUtils.getTypeMapping(resource));

    ReindexRequest.Builder builder = new ReindexRequest.Builder();
    builder.source(s -> s.index(sourceAlias))
        .dest(d -> d.index(newIndexName))
        .timeout(t -> t.time("10m"))
        .refresh(true);

    String script = getScript(resource);

    if (script != null) {
      builder.script(sc -> sc.inline(s -> s.source(script)));
    }

    var cf = esAsyncClient.reindex(builder.build())
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to reindex. newIndexName: {}, "
                + "sourceAlias: {}", newIndexName, sourceAlias, exception);
          } else {
            LOGGER.info("Reindex completed successfully for {}", newIndexName);
          }
        });

    ReindexResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to reindex. newIndexName: {}, "
          + "sourceAlias: {}", newIndexName, sourceAlias, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    return response;
  }

  private void prepareReindex(String newIndexName, String sourceAlias) {
    var tempIndexName = newIndexName + "_temp";
    if (Boolean.TRUE.equals(cloneIndex(tempIndexName, newIndexName).acknowledged())
        && updateAliases(tempIndexName, sourceAlias, Action.Kind.Add).acknowledged()
        && updateAliases(newIndexName, sourceAlias, Action.Kind.Remove).acknowledged()
        && deleteIndex(newIndexName).acknowledged()) {
      LOGGER.info("Successfully cloned index: {} into temp: {} and is ready for reindex",
          newIndexName, tempIndexName);
    } else {
      LOGGER.error("Failed to prepare temporary index {} for reindex of index {}.",
          tempIndexName, newIndexName);
      throw ExceptionsHelper.convertToElastic(new RuntimeException("Failed Reindex"));
    }
  }

  private String getScript(Class<? extends GeneratedMessageV3> resource) {
    String painlessScript = null;
    String indexName = IndexUtils.getIndexName(resource);

    Optional<ClassPathResourceLoader> optionalLoader =
        new ResourceResolver().getLoader(ClassPathResourceLoader.class);
    if (optionalLoader.isPresent()) {
      ClassPathResourceLoader loader = optionalLoader.get();
      var path = "classpath:migrations/" + indexName.split("-")[0] + "/"
          + indexName.split("-")[1];
      Optional<InputStream> optionalScript = loader.getResourceAsStream(path);

      if (optionalScript.isPresent()) {
        LOGGER.info("Painless script found");

        try {
          painlessScript = new String(optionalScript.get().readAllBytes());
        } catch (IOException e) {
          LOGGER.error("painless script failed to load for path: {}", path, e);
        }
      } else {
        LOGGER.info("No painless script found");
      }
    }

    return painlessScript;
  }

  /**
   * Updates Elasticsearch mappings for the given resource.
   *
   * @param resource Protobuf Message
   * @return Boolean result
   */
  public Boolean updateMappings(Class<? extends GeneratedMessageV3> resource) {
    var alias = IndexUtils.getAlias(resource);
    var mappings = IndexUtils.getTypeMapping(resource);

    var cf = esAsyncClient.indices().putMapping(pm -> pm.index(alias)
            .properties(mappings.properties()))
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to update mappings. alias: {}, "
                + "mappings: {}", alias, mappings, exception);
          } else {
            LOGGER.info("Mappings updated successfully for {}", alias);
          }
        });

    PutMappingResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to update mappings. alias: {}, "
          + "mappings: {}", alias, mappings, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error("Mappings failed to update.", e);
    }

    return response != null && response.acknowledged();
  }

  /**
   * Returns a HashMap of Elasticsearch Mappings in the current index for the given resource.  This
   * is used to compare existing mappings to what is generated from latest proto resources.
   *
   * @param resource Protobuf message resource used in the platform
   * @return HashMap of existing mappings from the given index
   */
  public TypeMapping existingMappings(Class<? extends GeneratedMessageV3> resource) {
    var alias = IndexUtils.getAlias(resource);
    var cf = esAsyncClient.indices().getMapping(r -> r.index(alias))
        .whenComplete((resp, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to get existing mappings. alias: {}",
                alias, exception);
          } else {
            LOGGER.info("Existing mappings retrieved successfully for {}", alias);
          }
        });

    GetMappingResponse getMappingsResponse = null;

    try {
      getMappingsResponse = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to get existing mappings. alias: {}",
          alias, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    TypeMapping response = null;
    if (getMappingsResponse != null) {
      var indexName = getMappingsResponse.result().keySet().iterator().next();
      response = getMappingsResponse.get(indexName).mappings();
    }

    return response;
  }

  private Boolean isMatchMappings(Class<? extends GeneratedMessageV3> resource) {
    var existingMappings = existingMappings(resource);
    var currentMappings = IndexUtils.getTypeMapping(resource);

    return MappingsComparator.compareMappings(existingMappings.properties(),
        currentMappings.properties(), "", true);
  }

  /**
   * Returns a {@link HealthResponse} for the current cluster.  From this we can get
   * <pre>{@code {
   *   "cluster_name": "elasticsearch",
   *   "status": "green",
   *   "timed_out": false,
   *   "number_of_nodes": 3,
   *   "number_of_data_nodes": 3,
   *   "active_primary_shards": 22,
   *   "active_shards": 50,
   *   "relocating_shards": 0,
   *   "initializing_shards": 0,
   *   "unassigned_shards": 0,
   *   "delayed_unassigned_shards": 0,
   *   "number_of_pending_tasks": 0,
   *   "number_of_in_flight_fetch": 0,
   *   "task_max_waiting_in_queue_millis": 0,
   *   "active_shards_percent_as_number": 100.0
   * }}</pre>
   *
   * @return ClusterHealthResponse
   */
  public HealthResponse clusterHealth() {
    var cf = esAsyncClient.cluster().health().whenComplete((response, exception) -> {
      if (exception != null) {
        LOGGER.error("Exception while trying to get the cluster health", exception);
      } else {
        LOGGER.info("Cluster health: {}", response);
      }
    });

    HealthResponse clusterHealthResponse = null;

    try {
      clusterHealthResponse = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to get the cluster health.", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    if (clusterHealthResponse == null) {
      LOGGER.info("Something is wrong, no response was received from the Elasticsearch Cluster");
    }

    return clusterHealthResponse;
  }

  /**
   * Update an Alias for Elasticsearch Indices.  Aliases are used to point to an Index or set of
   * Indices.
   *
   * @param indexNames String[] array of String Index Names
   * @param aliasName  String alias name
   * @param actionKind Action.Kind
   * @return UpdateAliasesResponse
   * @see ActionVariant
   * @see UpdateAliasesResponse
   */
  public UpdateAliasesResponse updateAliases(List<String> indexNames, String aliasName,
                                             Action.Kind actionKind) {
    Action.Builder actionBuilder = new Action.Builder();

    switch (actionKind) {
      case Add:
        actionBuilder.add(r -> r.indices(indexNames).alias(aliasName));
        break;
      case Remove:
        actionBuilder.remove(r -> r.indices(indexNames).alias(aliasName));
        break;
      case RemoveIndex:
        actionBuilder.removeIndex(r -> r.indices(indexNames));
        break;
      default:
        throw new IllegalArgumentException("Invalid actionKind: " + actionKind);
    }

    var cf = esAsyncClient.indices().updateAliases(r -> r.actions(actionBuilder.build()))
        .whenComplete((response, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to update aliases", exception);
          } else {
            LOGGER.info("Aliases updated: {}", response);
          }
        });

    UpdateAliasesResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to update aliases. indexNames: {}, "
          + "alias: {}, actionKind: {}", indexNames, aliasName, actionKind, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    return response;
  }

  public UpdateAliasesResponse updateAliases(String indexName, String aliasName,
                                             Action.Kind actionKind) {
    return updateAliases(Collections.singletonList(indexName), aliasName, actionKind);
  }

  /**
   * Check if an alias exists.
   *
   * @param aliasName String alias name to check exists
   * @return Boolean if alias exists or not
   */
  public BooleanResponse aliasExists(String aliasName) {
    var cf = esAsyncClient.indices().existsAlias(r -> r.name(aliasName))
        .whenComplete((response, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to verify Alias", exception);
          } else {
            LOGGER.info("Alias exists: {}", response);
          }
        });

    BooleanResponse exists = null;

    try {
      exists = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to verify Alias. alias: {}", aliasName, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    return exists;
  }

  /**
   * Gets index names by alias.  An alias may exist on one or more indices.  When managing aliases
   * sometimes we need to know which indices are currently aliased.
   *
   * @param alias String alias name
   * @return String[] array of index names associated with the alias
   */
  public List<String> indexNamesFromAlias(String alias) {
    var cf = esAsyncClient.indices().getAlias(r -> r.name(alias))
        .whenComplete((response, exception) -> {
          if (exception != null) {
            LOGGER.error("Exception while trying to get index names from an alias", exception);
          } else {
            LOGGER.info("Index names from alias: {}", response);
          }
        });

    GetAliasResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error("Thread was interrupted while trying to get index names from an alias. "
          + "alias: {}", alias, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExceptionsHelper.convertToElastic(e);
    }

    List<String> indexNameList = new ArrayList<>();
    if (response != null) {
      indexNameList = response.result().keySet().stream().toList();
    }

    return indexNameList;
  }
}
