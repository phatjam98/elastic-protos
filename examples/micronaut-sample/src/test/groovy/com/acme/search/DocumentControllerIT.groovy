package com.acme.search

import com.phatjam98.elasticprotos.engine.SearchEngine
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import java.time.Duration
import org.testcontainers.DockerClientFactory
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.Stepwise

/**
 * End to end against a real Elasticsearch: the startup bootstrap creates the indices, the seeder
 * indexes the samples and the controller serves them. Skipped when Docker is not available.
 */
@Stepwise
@MicronautTest
@Requires({ DockerClientFactory.instance().isDockerAvailable() })
class DocumentControllerIT extends Specification implements TestPropertyProvider {

  static final String IMAGE = 'docker.elastic.co/elasticsearch/elasticsearch:9.5.5'

  // Started from getProperties(), which runs before the application context; Testcontainers'
  // Ryuk container removes it when the JVM exits.
  static ElasticsearchContainer container

  @Inject
  @Client('/')
  HttpClient client

  @Inject
  SearchEngine engine

  @Override
  Map<String, String> getProperties() {
    if (container == null) {
      container = new ElasticsearchContainer(DockerImageName.parse(IMAGE))
          .withEnv('xpack.security.enabled', 'false')
          .withEnv('ES_JAVA_OPTS', '-Xms512m -Xmx512m')
          .withStartupTimeout(Duration.ofMinutes(3))
      container.start()
    }
    [
        'elastic-protos.engine': 'elasticsearch',
        'elastic-protos.hosts' : 'http://' + container.httpHostAddress,
        'seed.enabled'         : 'true',
    ]
  }

  def 'the startup bootstrap created both aliases'() {
    expect:
    engine.indexExists('documents')
    engine.indexExists('firms')
    engine.getAliases('documents').values().flatten().contains('documents')
  }

  def 'a full-text search finds the seeded documents, best match first'() {
    when:
    Map page = client.toBlocking().retrieve(
        HttpRequest.GET('/documents/search?q=merger%20agreement'), Map)

    then:
    page.total == 2
    page.totalExact
    page.hits*.id == ['doc-1', 'doc-3']
    page.hits[0].document.title == 'Merger agreement between Acme and Globex'
    // IGNORE fields are never sent to the engine, so they do not come back.
    !page.hits[0].document.containsKey('internalNotes')
  }

  def 'POST indexes a document that GET and search then return'() {
    when:
    def created = client.toBlocking().exchange(HttpRequest.POST('/documents', '''{
        "id": "doc-10", "firmId": "firm-3", "title": "Settlement agreement",
        "body": "The parties settle all claims.", "type": "FILING"}'''), Map)

    then:
    created.status == HttpStatus.CREATED
    created.body().id == 'doc-10'

    when:
    Map doc = client.toBlocking().retrieve(HttpRequest.GET('/documents/doc-10'), Map)
    Map page = client.toBlocking().retrieve(HttpRequest.GET('/documents/search?q=settle'), Map)

    then:
    doc.title == 'Settlement agreement'
    page.hits*.id == ['doc-10']
  }

  def 'POST /_bulk indexes an array and pages through the results'() {
    when:
    def response = client.toBlocking().exchange(HttpRequest.POST('/documents/_bulk', '''[
        {"id": "bulk-1", "title": "Lease agreement one"},
        {"id": "bulk-2", "title": "Lease agreement two"},
        {"id": "bulk-3", "title": "Lease agreement three"}]'''), Map)

    then:
    response.status == HttpStatus.ACCEPTED
    response.body().submitted == 3
    response.body().batches >= 1
    response.body().indexed == 3
    !response.body().errors   // empty lists are left out of the JSON

    when:
    Map first = client.toBlocking().retrieve(
        HttpRequest.GET('/documents/search?q=lease&size=2&from=0'), Map)
    Map second = client.toBlocking().retrieve(
        HttpRequest.GET('/documents/search?q=lease&size=2&from=2'), Map)

    then:
    first.total == 3
    first.hits.size() == 2
    second.hits.size() == 1
    (first.hits*.id + second.hits*.id).toSet() == ['bulk-1', 'bulk-2', 'bulk-3'] as Set
  }

  def 'DELETE removes a document and a second DELETE is a 404'() {
    when:
    def deleted = client.toBlocking().exchange(HttpRequest.DELETE('/documents/doc-10'))

    then:
    deleted.status == HttpStatus.NO_CONTENT

    when: 'reading it back right away'
    client.toBlocking().exchange(HttpRequest.GET('/documents/doc-10'))

    then:
    HttpClientResponseException gone = thrown()
    gone.status == HttpStatus.NOT_FOUND

    when:
    client.toBlocking().exchange(HttpRequest.DELETE('/documents/doc-10'))

    then:
    HttpClientResponseException e = thrown()
    e.status == HttpStatus.NOT_FOUND
  }

  def 'unknown ids are a 404 and malformed JSON is a 400'() {
    when:
    client.toBlocking().exchange(HttpRequest.GET('/documents/nope'))

    then:
    HttpClientResponseException missing = thrown()
    missing.status == HttpStatus.NOT_FOUND

    when:
    client.toBlocking().exchange(HttpRequest.POST('/documents', '{"bogus": true}'),
        Argument.of(Map), Argument.of(Map))

    then:
    HttpClientResponseException bad = thrown()
    bad.status == HttpStatus.BAD_REQUEST
    bad.response.getBody(Map).get().error.contains('bogus')
  }
}
