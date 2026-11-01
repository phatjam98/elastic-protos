package com.phatjam98.elasticsearch.utils

import org.apache.commons.codec.digest.DigestUtils
import spock.lang.Specification
import spock.lang.Unroll

class DocIdUtilsSpec extends Specification {
    void setup() {
    }

    void cleanup() {
    }

    @Unroll
    def "getDocIdHash when an arbitrary number of arguments are provided, testCase: #testCase"() {
        expect:
        DocIdUtils.getDocIdHash(*arguments) == expectedHash

        where:
        arguments                        | expectedHash                              | testCase
        ["singleArgument"]               | DigestUtils.md5Hex("singleArgument")      | "single arg"
        ["arg1", "arg2", "arg3", "arg4"] | DigestUtils.md5Hex("arg1-arg2-arg3-arg4") | "multiple args"
        ["a", "b", "c"]                  | DigestUtils.md5Hex("a-b-c")               | "3 args"
    }

    @Unroll
    def "GetDocId when an arbitrary number of arguments are provided, testCase: #testCase"() {
        expect:
        DocIdUtils.getDocId(*arguments) == expected

        where:
        arguments                        | expected              | testCase
        ["singleArgument"]               | "singleArgument"      | "single arg"
        ["arg1", "arg2", "arg3", "arg4"] | "arg1-arg2-arg3-arg4" | "multiple args"
        ["a", "b", "c"]                  | "a-b-c"               | "3 args"
    }

    def "GetUserNotificationDocId"() {
        when:
        var id = DocIdUtils.getUserNotificationDocId(userId, locationId, date)

        then:
        id == expected

        where:
        userId  | locationId  | date                             | expected
        "user1" | "location1" | (int) System.currentTimeMillis() | userId + "-" + locationId + "-" + date
    }

    def "GetNotificationDocId"() {
        when:
        var id = DocIdUtils.getNotificationDocId(locationId, date)

        then:
        id == expected

        where:
        locationId  | date                             | expected
        "location1" | (int) System.currentTimeMillis() | locationId + "-" + date

    }
}
