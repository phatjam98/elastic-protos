package com.phatjam98.elasticprotos.engine.rest

import com.phatjam98.elasticprotos.util.Json

/** A scripted transport: canned responses are consumed in order and every request is recorded. */
class FakeHttp implements RawHttp {

  static class Recorded {
    String method
    String path
    Map<String, String> params
    String body
    String contentType

    Map json() { body == null ? null : Json.parseObject(body) }
  }

  final List<Recorded> requests = []
  /** Canned responses, or IOExceptions to throw for that request. */
  final Deque<Object> responses = new ArrayDeque<>()
  boolean closed
  IOException failure

  FakeHttp enqueue(int status, Object body = null) {
    responses.add(new RawHttp.Response(status, body == null ? '' : (body instanceof String ? body : Json.canonical(body))))
    this
  }

  /** Makes the next request fail with an IOException after it is recorded. */
  FakeHttp enqueueFailure(IOException e) {
    responses.add(e)
    this
  }

  @Override
  RawHttp.Response perform(String method, String path, Map<String, String> params, String body, String contentType) {
    if (failure) {
      throw failure
    }
    requests << new Recorded(method: method, path: path, params: params, body: body, contentType: contentType)
    assert !responses.isEmpty(): "no canned response for ${method} ${path}"
    def next = responses.poll()
    if (next instanceof IOException) {
      throw next
    }
    next as RawHttp.Response
  }

  Recorded getLast() { requests.last() }

  @Override
  void close() {
    closed = true
  }
}
