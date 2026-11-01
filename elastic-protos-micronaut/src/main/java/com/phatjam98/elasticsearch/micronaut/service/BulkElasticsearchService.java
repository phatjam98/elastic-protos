package com.phatjam98.elasticsearch.micronaut.service;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import io.micronaut.context.annotation.Property;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for Bulk actions on Elasticsearch.
 */
public class BulkElasticsearchService extends ElasticsearchService {
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkElasticsearchService.class);

  @Property(name = "elasticsearch.bulk.bulkActions")
  private int bulkActions;

  @Property(name = "elasticsearch.bulk.concurrentRequests")
  private int concurrentRequests;

  @Property(name = "elasticsearch.bulk.flushInterval")
  private int flushInterval;

  /**
   * Set replicas and shards based on the Elasticsearch {@link HealthResponse}.  Set replicas
   * to 1 less than total number of Data Nodes.
   *
   * @param client ElasticsearchAsyncClient
   */
  public BulkElasticsearchService(ElasticsearchAsyncClient client) {
    super(client);
  }

  /**
   * Set replicas and shards based on the Elasticsearch {@link HealthResponse}.
   * This is used for testing purposes.
   *
   * @param client             ElasticsearchAsyncClient
   * @param bulkActions        BulkActions
   * @param concurrentRequests ConcurrentRequests
   * @param flushInterval      FlushInterval
   */
  public BulkElasticsearchService(ElasticsearchAsyncClient client, int bulkActions,
                                  int concurrentRequests, int flushInterval) {
    super(client);
    this.bulkActions = bulkActions;
    this.concurrentRequests = concurrentRequests;
    this.flushInterval = flushInterval;
  }

  /**
   * Bulk Request to Elasticsearch with a preformed {@link BulkRequest}.
   *
   * @param bulkRequest BulkRequest
   * @return BulkResponse indicating status of the request
   */
  public BulkResponse bulk(BulkRequest bulkRequest) {
    var cf = esAsyncClient.bulk(bulkRequest)
        .whenComplete((bulkResponse, throwable) -> {
          if (throwable != null) {
            LOGGER.error("Error while trying to bulk update documents.", throwable);
          }
        });

    BulkResponse response = null;

    try {
      response = cf.get();
    } catch (InterruptedException e) {
      LOGGER.error(
          "Thread was interrupted while trying to bulk update documents.", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOGGER.error(
          "Execution exception while trying to bulk update documents.", e);
    }

    return response;
  }

  /**
   * Bulk API access to create a group of documents in the given index.
   *
   * @param indexName String name of the index.
   * @param jsonDocs  List of Strings jsonDocs to create in bulk.
   * @return BulkResponse
   */
  public BulkResponse bulkCreate(String indexName, List<String> jsonDocs) {
    BulkResponse bulkResponse = null;
    BulkRequest.Builder bulkRequest = new BulkRequest.Builder();
    bulkRequest.index(indexName);

    for (String doc : jsonDocs) {
      BinaryData data = BinaryData.of(doc.getBytes(), ContentType.APPLICATION_JSON);
      bulkRequest.operations(op -> op.index(idx -> idx.index(indexName).document(data)));
    }

    bulkResponse = bulk(bulkRequest.build());

    return bulkResponse;
  }

  /**
   * Gets a {@link BulkIngester} for the given bulk processor name.
   *
   * @return BulkProcessor
   */
  public BulkIngester<String> getBulkProcessor() {
    BulkListener<String> bulkListener = new BulkListener<>() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request, List list) {
        //TODO: Add logging and metrics here
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, List list,
                            BulkResponse response) {
        //TODO: Add logging and metrics here
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, List list, Throwable failure) {
        //TODO: Add logging and metrics here
      }
    };

    return BulkIngester.of(bl -> bl.client(esAsyncClient)
        .flushInterval(flushInterval, TimeUnit.MILLISECONDS)
        .maxConcurrentRequests(bulkActions)
        .maxConcurrentRequests(concurrentRequests)
        .listener(bulkListener)
    );
  }
}
