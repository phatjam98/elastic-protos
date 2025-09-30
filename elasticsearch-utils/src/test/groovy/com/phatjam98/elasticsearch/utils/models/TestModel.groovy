package com.phatjam98.elasticsearch.utils.models

import com.fasterxml.jackson.annotation.JsonProperty

class TestModel {

    String id

    @JsonProperty("user_name")
    String userName

    @JsonProperty("user_type")
    Long userType

    @JsonProperty("created_at")
    long createdAt

    String getId() {
        return id
    }

    TestModel setId(String id) {
        this.id = id
        return this;
    }

    String getUserName() {
        return userName
    }

    TestModel setUserName(String userName) {
        this.userName = userName
        return this;
    }

    Long getUserType() {
        return userType
    }

    TestModel setUserType(Long userType) {
        this.userType = userType
        return this;
    }

    long getCreatedAt() {
        return createdAt
    }

    TestModel setCreatedAt(long createdAt) {
        this.createdAt = createdAt
        return this;
    }

    static Map<String, Object> esMappings() {
        Map<String, Object> properties = new HashMap<>()
        properties.put("id", Collections.singletonMap("type", "keyword"))
        properties.put("user_name", Collections.singletonMap("type", "keyword"))
        properties.put("user_type", Collections.singletonMap("type", "long"))
        properties.put("created_at", Collections.singletonMap("type", "date"))

        Map<String, Object> mappings = new HashMap<>()
        mappings.put("properties", properties)

        return mappings
    }
}
