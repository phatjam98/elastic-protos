package com.phatjam98.elasticprotos.elasticsearch

import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine
import org.elasticsearch.client.RestClient
import org.apache.http.HttpHost
import spock.lang.Specification

class ElasticsearchEngineSpec extends Specification {

  def "builds an engine from connection settings without connecting"() {
    when:
    def engine = ElasticsearchEngine.builder()
        .hosts('http://localhost:19200', 'https://other:9200')
        .basicAuth('elastic', 'secret')
        .apiKey('id:key')
        .insecureTrustAllCertificates(true)
        .build()

    then:
    engine instanceof JsonRestEngine

    cleanup:
    engine?.close()
  }

  def "a custom SSL context is accepted"() {
    when:
    def engine = ElasticsearchEngine.builder().hosts('https://localhost:19200').sslContext(javax.net.ssl.SSLContext.getDefault()).apiKey('cHJlZW5jb2RlZA==').build()

    then:
    engine != null

    cleanup:
    engine?.close()
  }

  def "at least one host is required"() {
    when:
    ElasticsearchEngine.builder().build()

    then:
    thrown(IllegalStateException)
  }

  def "wraps an existing client and reports connection failures as IO errors"() {
    given:
    def engine = ElasticsearchEngine.of(RestClient.builder(new HttpHost('localhost', 1)).build())

    when:
    engine.indexExists('x')

    then:
    def e = thrown(EngineException)
    e.kind == EngineException.Kind.IO
    e.message.startsWith('HEAD /x on http://localhost:1 failed: ')

    cleanup:
    engine.close()
  }

  def "the transport describes every node of the client"() {
    given:
    def client = RestClient.builder(new HttpHost('localhost', 9200), new HttpHost('search.example', 9243, 'https')).build()
    def http = new RestClientHttp(client)

    expect:
    http.describe() == 'http://localhost:9200, https://search.example:9243'

    cleanup:
    http.close()
  }
}
