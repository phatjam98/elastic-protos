package com.phatjam98.elasticsearch.utils;

import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.GeoDistanceType;
import co.elastic.clients.elasticsearch._types.ScriptSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.phatjam98.core.common.proto.FlatStructProtos;
import com.phatjam98.elasticsearch.utils.models.AnyInField;
import com.phatjam98.elasticsearch.utils.models.ContainsField;
import com.phatjam98.elasticsearch.utils.models.EqualityField;
import com.phatjam98.elasticsearch.utils.models.ExistsField;
import com.phatjam98.elasticsearch.utils.models.Field;
import com.phatjam98.elasticsearch.utils.models.FieldValue;
import com.phatjam98.elasticsearch.utils.models.MatchesField;
import com.phatjam98.elasticsearch.utils.models.QueryObject;
import com.phatjam98.elasticsearch.utils.models.RangeField;
import com.phatjam98.elasticsearch.utils.models.geo.BoundingBoxField;
import com.phatjam98.elasticsearch.utils.models.geo.DistanceField;
import com.phatjam98.elasticsearch.utils.models.geo.ShapeField;
import com.phatjam98.protos.service.protos.FieldCondition;
import com.phatjam98.protos.service.protos.GeoDistanceSort;
import com.phatjam98.protos.service.protos.Pagination;
import com.phatjam98.protos.service.protos.SearchCondition;
import com.phatjam98.protos.service.protos.SearchCriteria;
import com.phatjam98.protos.service.protos.SearchOperationType;
import com.phatjam98.protos.service.protos.SortingCriteria;
import com.phatjam98.protos.service.protos.SortingOrder;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Utility to build Elasticsearch SearchRequests from SearchCriteria Protobufs.  This also
 * includes helpers for various components of a SearchRequest or SearchCriteria.
 */
@Singleton
public class RequestBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(RequestBuilder.class);

  static final String UNRECOGNIZED = "UNRECOGNIZED";
  static final String PIT_KEEP_ALIVE = "1m";

  /**
   * Takes in an indexName and {@link SearchCriteria} to construct the necessary SearchRequest.
   *
   * @param indexName      String name of the index
   * @param searchCriteria SearchCriteria Proto used to construct the SearchRequest
   * @return SearchRequest used by elasticsearch service
   */
  public SearchRequest buildRequest(String indexName,
                                    SearchCriteria searchCriteria) {
    return buildRequest(Collections.singletonList(indexName), searchCriteria);
  }

  /**
   * Takes in indexNames and {@link SearchCriteria} to construct the necessary SearchRequest for
   * ElasticsearchService#search(SearchRequest).
   *
   * @param indexNames     List of Strings names of the indices to search
   * @param searchCriteria SearchCriteria Proto used to construct the SearchRequest
   * @return SearchRequest used by elasticsearch service
   */
  public SearchRequest buildRequest(List<String> indexNames, SearchCriteria searchCriteria) {
    SearchRequest.Builder searchBuilder = new SearchRequest.Builder();
    Query query = buildQuery(searchCriteria);
    searchBuilder.query(query);

    if (searchCriteria.hasPagination()) {
      setPagination(searchCriteria.getPagination(), searchBuilder);
    }

    if (!searchCriteria.getSortingCriteriaList().isEmpty()) {
      addSorts(searchCriteria, searchBuilder);
    }

    searchBuilder.index(indexNames);

    return searchBuilder.build();
  }

  /**
   * Takes {@link SearchCriteria} and {@link SearchRequest.Builder} adding any
   * {@link SortingCriteria} to the SearchSourceBuilder.
   *
   * @param searchCriteria SearchCriteria
   * @param searchBuilder  SearchBuilder
   */
  public void addSorts(SearchCriteria searchCriteria,
                       SearchRequest.Builder searchBuilder) {
    List<SortOptions> sortBuilderList = new ArrayList<>();

    for (SortingCriteria sortingCriteria : searchCriteria.getSortingCriteriaList()) {
      if (sortingCriteria.hasFieldSort()
          && !sortingCriteria.getFieldSort().getFieldName().isEmpty()) {
        addFieldSort(sortBuilderList, sortingCriteria);
      } else if (sortingCriteria.hasScoreSort()) {
        addScoreSort(sortBuilderList, sortingCriteria);
      } else if (sortingCriteria.hasGeoDistanceSort()) {
        addGeoDistanceSort(sortBuilderList, sortingCriteria);
      } else if (sortingCriteria.hasScriptSort()) {
        addScriptSort(sortBuilderList, sortingCriteria);
      }
    }


    searchBuilder.sort(sortBuilderList);
  }

  /**
   * Takes a Collection of {@link SortOptions} and {@link SortingCriteria} to construct and add
   * any Script Sort.
   *
   * @param sortBuilderList List of SortOptions
   * @param sortingCriteria SortingCriteria
   */
  public void addScriptSort(List<SortOptions> sortBuilderList,
                            SortingCriteria sortingCriteria) {
    var scriptSort = sortingCriteria.getScriptSort();
    var scriptOrId = scriptSort.getScriptOrId();

    ScriptSort.Builder sortBuilder = new ScriptSort.Builder();
    sortBuilder.script(s -> s.inline(i -> i.source(scriptOrId)));

    SortOptions.Builder sortOptions = new SortOptions.Builder();
    sortOptions.script(sortBuilder.build());

    sortBuilderList.add(sortOptions.build());
  }

  /**
   * Takes a Collection of {@link SortOptions} and {@link SortingCriteria} to construct and add
   * any GeoDistance Sort.
   *
   * @param sortBuilderList List of SortOptions
   * @param sortingCriteria SortingCriteria
   */
  public void addGeoDistanceSort(List<SortOptions> sortBuilderList,
                                 SortingCriteria sortingCriteria) {
    var geoDistanceSort = sortingCriteria.getGeoDistanceSort();
    GeoDistanceType geoDistanceType = getGeoDistanceName(geoDistanceSort);
    DistanceUnit distanceUnit = getDistanceUnitName(geoDistanceSort);
    var order = getOrderName(geoDistanceSort.getOrder());
    var fieldName = geoDistanceSort.getFieldName();
    var lat = geoDistanceSort.getPoint().getLat();
    var lon = geoDistanceSort.getPoint().getLon();

    SortOptions.Builder sortOptions = new SortOptions.Builder();

    if (!fieldName.isEmpty() && lat != 0 && lon != 0) {
      sortOptions.geoDistance(g -> g.field(fieldName)
          .unit(distanceUnit)
          .distanceType(geoDistanceType)
          .location(l -> l.latlon(ll -> ll.lat(lat).lon(lon)))
          .order(order));

      sortBuilderList.add(sortOptions.build());
    }
  }

  /**
   * Returns the name of the DistanceType.
   *
   * @param geoDistanceSort GeoDistanceSort
   * @return GeoDistanceType name of sort
   */
  public GeoDistanceType getGeoDistanceName(GeoDistanceSort geoDistanceSort) {
    GeoDistanceType geoDistance;
    switch (geoDistanceSort.getDistanceType()) {
      case ARC:
        geoDistance = GeoDistanceType.Arc;
        break;
      case PLANE:
        geoDistance = GeoDistanceType.Plane;
        break;
      default:
        geoDistance = GeoDistanceType.Arc;
        break;
    }

    return geoDistance;
  }

  /**
   * Returns name of the DistanceUnit.
   *
   * @param geoDistanceSort GeoDistanceSort
   * @return String name of DistanceUnit
   */
  public DistanceUnit getDistanceUnitName(GeoDistanceSort geoDistanceSort) {
    DistanceUnit distanceUnit = null;
    var unit = geoDistanceSort.getDistanceUnit();
    switch (unit) {
      case INCH:
        distanceUnit = DistanceUnit.Inches;
        break;
      case FEET:
        distanceUnit = DistanceUnit.Feet;
        break;
      case YARD:
        distanceUnit = DistanceUnit.Yards;
        break;
      case MILES:
        distanceUnit = DistanceUnit.Miles;
        break;
      case NAUTICALMILES:
        distanceUnit = DistanceUnit.NauticMiles;
        break;
      case MILLIMETERS:
        distanceUnit = DistanceUnit.Millimeters;
        break;
      case CENTIMETERS:
        distanceUnit = DistanceUnit.Centimeters;
        break;
      case METERS:
        distanceUnit = DistanceUnit.Meters;
        break;
      case KILOMETERS:
        distanceUnit = DistanceUnit.Kilometers;
        break;
      default:
        distanceUnit = DistanceUnit.Kilometers;
        break;
    }

    return distanceUnit;
  }

  /**
   * Returns {@link SortingOrder} String name.
   *
   * @param sortingOrder SortingOrder
   * @return String order name
   */
  public SortOrder getOrderName(SortingOrder sortingOrder) {
    SortOrder order;

    switch (sortingOrder) {
      case ASC:
        order = SortOrder.Asc;
        break;
      case DESC:
        order = SortOrder.Desc;
        break;
      default:
        order = SortOrder.Asc;
        break;
    }

    return order;
  }

  /**
   * Takes a Collection of {@link SortOptions} and a {@link SortingCriteria}.  Creates and adds
   * ScoreSorts to the SortBuilderList.
   *
   * @param sortBuilderList List of SortOptions
   * @param sortingCriteria SortingCriteria
   */
  public void addScoreSort(List<SortOptions> sortBuilderList, SortingCriteria sortingCriteria) {
    var order = getOrderName(sortingCriteria.getScoreSort().getOrder());
    SortOptions.Builder sortOptions = new SortOptions.Builder();
    sortOptions.score(s -> s.order(order));

    sortBuilderList.add(sortOptions.build());
  }

  /**
   * Takes a Collection of {@link SortOptions} and a {@link SortingCriteria} to create and add
   * {@link com.phatjam98.protos.service.protos.FieldSort} to the SortBuilder.
   *
   * @param sortBuilderList List of SortBuilder
   * @param sortingCriteria SortingCriteria
   */
  public void addFieldSort(List<SortOptions> sortBuilderList, SortingCriteria sortingCriteria) {
    var fieldSort = sortingCriteria.getFieldSort();
    var fieldName = fieldSort.getFieldName();
    var order = getOrderName(fieldSort.getOrder());
    var sortOptions = new SortOptions.Builder();

    co.elastic.clients.elasticsearch._types.FieldSort.Builder fieldSortBuilder =
        new co.elastic.clients.elasticsearch._types.FieldSort.Builder();
    fieldSortBuilder.field(fieldName).order(order);

    if (fieldName.contains(".")) {
      var pathRoot = fieldName.substring(0, fieldName.lastIndexOf("."));
      fieldSortBuilder.nested(n -> n.path(pathRoot));
    }

    sortOptions.field(fieldSortBuilder.build());

    sortBuilderList.add(sortOptions.build());
  }

  /**
   * Takes {@link Pagination} and {@link SearchRequest.Builder} to create and add pagination to the
   * SearchSourceBuilder.
   *
   * @param pagination Pagination
   * @param builder    SearchSourceBuilder
   */
  public void setPagination(Pagination pagination, SearchRequest.Builder builder) {
    var size = pagination.getSize();
    var from = pagination.getFrom();
    var pit = pagination.getCursor();

    if (size == -1) {
      size = 0;
    } else if (size == 0) {
      size = 10;
    }

    builder.size(size);
    if (pit.isEmpty()) {
      builder.from(Math.max(from, 0));
    } else {
      builder.pit(p -> p.id(pit).keepAlive(t -> t.time(PIT_KEEP_ALIVE)));
    }
  }

  /**
   * Used to produce a query from the provided
   * {@link SearchCriteria}.
   *
   * <pre>{@code {
   *   "from": 0,
   *   "size": 5,
   *   "query": {
   *     "bool": {
   *       "should": [
   *         {
   *           "bool": {
   *             "must": [
   *               {
   *                 "constant_score": {
   *                   "filter": {
   *                     "term": {
   *                       "name": {
   *                         "value": "John",
   *                         "boost": 1.0
   *                       }
   *                     }
   *                   },
   *                   "boost": 1.0
   *                 }
   *               }
   *             ],
   *             "adjust_pure_negative": true,
   *             "boost": 1.0
   *           }
   *         }
   *       ],
   *       "adjust_pure_negative": true,
   *       "boost": 1.0
   *     }
   *   },
   *   "sort": [
   *     {
   *       "forecasted_cases": {
   *         "order": "asc"
   *       }
   *     }
   *   ]
   * }}</pre>
   *
   * @param searchCriteria SearchCriteria used to create the queryBuilder
   * @return Query
   */
  public Query buildQuery(SearchCriteria searchCriteria) {
    Query.Builder queryBuilder = new Query.Builder();
    List<QueryObject> queryObjects = createQueryObjects(searchCriteria);

    if (queryObjects.isEmpty()) {
      queryBuilder.matchAll(ma -> ma);
    } else {
      List<Query> queries = new ArrayList<>();
      queryObjects.forEach(field -> queries.add(field.esBoolQuery()));
      queryBuilder.bool(b -> b.should(queries));
    }

    return queryBuilder.build();
  }

  public BoolQuery.Builder boolQueryBuilder(SearchCriteria searchCriteria, BoolType boolType) {
    BoolQuery.Builder queryBuilder = new BoolQuery.Builder();
    boolQueryBuilder(searchCriteria, boolType, queryBuilder);

    return queryBuilder;
  }

  /**
   * Takes a {@link SearchCriteria} and {@link BoolType} to create a {@link BoolQuery.Builder}.
   *
   * @param searchCriteria SearchCriteria
   * @param boolType       BoolType
   * @param queryBuilder   BoolQuery.Builder
   */
  public void boolQueryBuilder(SearchCriteria searchCriteria, BoolType boolType,
                               BoolQuery.Builder queryBuilder) {
    List<QueryObject> queryObjects = createQueryObjects(searchCriteria);
    List<Query> queries = new ArrayList<>();
    queryObjects.forEach(field -> queries.add(field.esBoolQuery()));

    switch (boolType) {
      case FILTER:
        queryBuilder.filter(queries);
        break;
      case MUST:
        queryBuilder.must(queries);
        break;
      case MUST_NOT:
        queryBuilder.mustNot(queries);
        break;
      default:
        queryBuilder.should(queries);
        break;
    }
  }

  public Query buildLayerQuery(List<String> layerNames, SearchCriteria searchCriteria) {
    Query.Builder queryBuilder = new Query.Builder();
    List<QueryObject> queryObjects = createQueryObjects(searchCriteria);
    List<co.elastic.clients.elasticsearch._types.FieldValue> layerFieldValues = new ArrayList<>();

    for (String name : layerNames) {
      var fv = new co.elastic.clients.elasticsearch._types.FieldValue.Builder()
          .stringValue(name).build();
      layerFieldValues.add(fv);
    }

    var indexFilter = QueryBuilders.terms().field("_index")
        .terms(tqf -> tqf.value(layerFieldValues)).build();

    List<Query> queries = new ArrayList<>();
    queryObjects.forEach(field -> queries.add(field.esBoolQuery()));
    queryBuilder.bool(b -> b.must(queries)
        .filter(indexFilter._toQuery()));

    return queryBuilder.build();
  }

  /**
   * Extracts {@link QueryObject}s from
   * {@link SearchCriteria}
   * to craft Elasticsearch queries.
   *
   * @param searchCriteria SearchCriteria from the request protobuf
   * @return List of QueryObjects
   */
  public List<QueryObject> createQueryObjects(SearchCriteria searchCriteria) {
    List<QueryObject> queryObjects = new ArrayList<>();
    List<SearchCondition> conditions = searchCriteria.getSearchConditionList();

    for (SearchCondition condition : conditions) {
      extractQueryObjects(queryObjects, condition);
    }

    return queryObjects;
  }

  /**
   * Takes a Collection of {@link QueryObject} and a {@link SearchCondition} to extract QueryObject
   * from the SearchCondition and add to the collection.
   *
   * @param queryObjects List of QueryObject
   * @param condition    SearchCondition
   */
  public void extractQueryObjects(List<QueryObject> queryObjects,
                                  SearchCondition condition) {
    QueryObject queryObject = new QueryObject();
    List<Field> fields = new ArrayList<>();
    List<FieldCondition> fieldConditions = condition.getFieldConditionList();

    for (FieldCondition fieldCondition : fieldConditions) {
      extractFields(fields, fieldCondition);
    }

    queryObject.setSearchFields(fields);
    queryObjects.add(queryObject);
  }

  /**
   * Takes a Collection of {@link Field} and a {@link FieldCondition} to extract fields from the
   * FieldCondition and add to the Field Collection.
   *
   * @param fields         List of Field
   * @param fieldCondition FieldCondition
   */
  public void extractFields(List<Field> fields,
                            FieldCondition fieldCondition) {
    Field field = createField(fieldCondition.getOperation());

    if (field != null) {
      field.setName(fieldCondition.getField());

      switch (fieldCondition.getValue().getKindCase()) {
        case STRING_VALUE:
          extractStringValue(fieldCondition, field);
          break;
        case NUMBER_VALUE:
          extractNumberValue(fieldCondition, field);
          break;
        case BOOL_VALUE:
          extractBoolValue(fieldCondition, field);
          break;
        case NULL_VALUE:
          extractNullValue(fieldCondition, field);
          break;
        case TIMESTAMP_VALUE:
          extractTimestampValue(fieldCondition, field);
          break;
        case RANGE_VALUE:
          extractRangeValue(fieldCondition, field);
          break;
        case LIST_VALUE:
          extractListValue(fieldCondition, field);
          break;
        case GEO_VALUE:
          extractGeoValue(fieldCondition, field);
          break;
        case GEO_DISTANCE_VALUE:
          extractGeoDistanceValue(fieldCondition, field);
          break;
        case GEO_BOUNDING_BOX_VALUE:
          extractGeoBoundingBoxValue(fieldCondition, field);
          break;
        default:
          String kind = fieldCondition.getValue().getKindCase().name();
          LOGGER.error("Invalid fieldCondition value kind: {}", kind);
          break;
      }

      fields.add(field);
    }
  }

  /**
   * Takes a {@link FieldCondition} and a {@link Field} to extract GeoBoundingBox Values from the
   * FieldCondition and set the Field.
   *
   * @param fieldCondition FieldCondition
   * @param field          Field
   */
  public void extractGeoBoundingBoxValue(FieldCondition fieldCondition,
                                         Field field) {
    if (field instanceof BoundingBoxField) {
      field.setValue(
          new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.GEO_BOUNDING_BOX_VALUE)
              .setValue(fieldCondition.getValue().getGeoBoundingBoxValue()));
    }
  }

  /**
   * Takes a {@link FieldCondition} and a {@link Field} to extract GeoDistance Values from the
   * FieldCondition and set the Field.
   *
   * @param fieldCondition FieldCondition
   * @param field          Field
   */
  public void extractGeoDistanceValue(FieldCondition fieldCondition,
                                      Field field) {
    if (field instanceof DistanceField) {
      field.setValue(
          new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.GEO_DISTANCE_VALUE)
              .setValue(fieldCondition.getValue().getGeoDistanceValue()));
    }
  }

  public void extractGeoValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.GEO_VALUE)
        .setValue(fieldCondition.getValue().getGeoValue()));
  }

  public void extractListValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.LIST_VALUE)
        .setValue(fieldCondition.getValue().getListValue()));
  }

  /**
   * Takes {@link FieldCondition} and {@link Field} to extract Range Values from the FieldCondition
   * and set the Field.
   *
   * @param fieldCondition FieldCondition
   * @param field          Field
   */
  public void extractRangeValue(FieldCondition fieldCondition, Field field) {
    if (field instanceof RangeField) {
      ((RangeField) field).setMinInclusive(false).setMaxInclusive(false)
          .setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.RANGE_VALUE)
              .setValue(fieldCondition.getValue().getRangeValue()));
    }
  }

  public void extractTimestampValue(FieldCondition fieldCondition,
                                    Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.TIMESTAMP_VALUE)
        .setValue(fieldCondition.getValue().getTimestampValue()));
  }

  public void extractNullValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.NULL_VALUE)
        .setValue(fieldCondition.getValue().getNullValue()));
  }

  public void extractBoolValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.BOOL_VALUE)
        .setValue(fieldCondition.getValue().getBoolValue()));
  }

  public void extractNumberValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.NUMBER_VALUE)
        .setValue(fieldCondition.getValue().getNumberValue()));
  }

  public void extractStringValue(FieldCondition fieldCondition, Field field) {
    field.setValue(new FieldValue().setKind(FlatStructProtos.FlatValue.KindCase.STRING_VALUE)
        .setValue(fieldCondition.getValue().getStringValue()));
  }

  /**
   * Takes a {@link SearchOperationType} to return the proper {@link Field} for the operation.
   *
   * @param operation SearchOperationType
   * @return Field
   */
  public Field createField(SearchOperationType operation) {
    Field field = null;

    switch (operation) {
      case EQUALS:
        field = new EqualityField();
        break;
      case EXISTS:
        field = new ExistsField();
        break;
      case TEXT_MATCHES:
        field = new MatchesField();
        break;
      case TEXT_CONTAINS:
        field = new ContainsField();
        break;
      case NUM_RANGE:
        field = new RangeField();
        break;
      case ANY_IN:
        field = new AnyInField();
        break;
      case GEO_BOUNDING_BOX:
        field = new BoundingBoxField();
        break;
      case GEO_DISTANCE:
        field = new DistanceField();
        break;
      case GEO_CONTAINED:
        field = new ShapeField(SearchOperationType.GEO_CONTAINED);
        break;
      case GEO_CONTAINS:
        field = new ShapeField(SearchOperationType.GEO_CONTAINS);
        break;
      case GEO_INTERSECTS:
        field = new ShapeField(SearchOperationType.GEO_INTERSECTS);
        break;
      default:
        LOGGER.error("Invalid fieldCondition Operation: {}", operation);
        break;
    }

    return field;
  }
}