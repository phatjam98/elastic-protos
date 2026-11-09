package com.phatjam98.elasticprotos.opensearch

import com.phatjam98.elasticprotos.engine.EngineException
import com.phatjam98.elasticprotos.engine.rest.JsonRestEngine
import org.apache.hc.core5.http.HttpHost
import org.opensearch.client.RestClient
import com.sun.net.httpserver.HttpServer
import java.nio.charset.StandardCharsets
import spock.lang.Specification

class OpenSearchEngineSpec extends Specification {

  def "builds an engine from connection settings without connecting"() {
    when:
    def engine = OpenSearchEngine.builder()
        .hosts('http://localhost:19200', 'https://other:9200')
        .basicAuth('admin', 'secret')
        .apiKey('id:key')
        .insecureTrustAllCertificates(true)
        .build()

    then:
    engine instanceof JsonRestEngine

    cleanup:
    engine?.close()
  }

  def "a custom SSL context and trust-all certificates are mutually exclusive"() {
    when:
    OpenSearchEngine.builder().hosts('https://localhost:19200')
        .sslContext(javax.net.ssl.SSLContext.getDefault())
        .insecureTrustAllCertificates(true)
        .build()

    then:
    def e = thrown(IllegalStateException)
    e.message.contains('mutually exclusive')
  }

  def "a custom SSL context with trust-all switched off is accepted"() {
    when:
    def engine = OpenSearchEngine.builder().hosts('https://localhost:19200')
        .sslContext(javax.net.ssl.SSLContext.getDefault())
        .insecureTrustAllCertificates(false)
        .build()

    then:
    engine != null

    cleanup:
    engine?.close()
  }

  def "response bodies without a declared charset are read as UTF-8"() {
    given:
    def server = HttpServer.create(new InetSocketAddress('localhost', 0), 0)
    server.createContext('/') { exchange ->
      byte[] bytes = 'caf\u00e9 \u2713'.getBytes(StandardCharsets.UTF_8)
      exchange.responseHeaders.add('Content-Type', 'text/plain')
      exchange.sendResponseHeaders(200, bytes.length)
      exchange.responseBody.withCloseable { it.write(bytes) }
    }
    server.start()
    def http = new RestClientHttp(RestClient.builder(new HttpHost('http', 'localhost', server.address.port)).build())

    when:
    def response = http.perform('GET', '/', [:], null, null)

    then:
    response.status() == 200
    response.body() == 'caf\u00e9 \u2713'

    cleanup:
    http?.close()
    server?.stop(0)
  }

  def "a custom SSL context is accepted"() {
    when:
    def engine = OpenSearchEngine.builder().hosts('https://localhost:19200').sslContext(javax.net.ssl.SSLContext.getDefault()).apiKey('cHJlZW5jb2RlZA==').build()

    then:
    engine != null

    cleanup:
    engine?.close()
  }

  def "at least one host is required and host URLs are validated"() {
    when:
    OpenSearchEngine.builder().build()

    then:
    thrown(IllegalStateException)

    when:
    OpenSearchEngine.builder().hosts('::not a url::')

    then:
    thrown(IllegalArgumentException)
  }

  def "wraps an existing client and reports connection failures as IO errors"() {
    given:
    def engine = OpenSearchEngine.of(RestClient.builder(new HttpHost('localhost', 1)).build())

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
    def client = RestClient.builder(new HttpHost('localhost', 9200), new HttpHost('https', 'search.example', 9243)).build()
    def http = new RestClientHttp(client)

    expect:
    http.describe() == 'http://localhost:9200, https://search.example:9243'

    cleanup:
    http.close()
  }
}
