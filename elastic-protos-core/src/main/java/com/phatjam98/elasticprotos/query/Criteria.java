package com.phatjam98.elasticprotos.query;

import com.phatjam98.elasticprotos.proto.DistanceUnit;
import com.phatjam98.elasticprotos.proto.FieldCondition;
import com.phatjam98.elasticprotos.proto.FieldSort;
import com.phatjam98.elasticprotos.proto.FlatValue;
import com.phatjam98.elasticprotos.proto.FullTextOptions;
import com.phatjam98.elasticprotos.proto.GeoDistanceSort;
import com.phatjam98.elasticprotos.proto.LatLon;
import com.phatjam98.elasticprotos.proto.Pagination;
import com.phatjam98.elasticprotos.proto.ScoreSort;
import com.phatjam98.elasticprotos.proto.SearchCondition;
import com.phatjam98.elasticprotos.proto.SearchCriteria;
import com.phatjam98.elasticprotos.proto.SearchOperationType;
import com.phatjam98.elasticprotos.proto.SortingCriteria;
import com.phatjam98.elasticprotos.proto.SortingOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A fluent builder for {@link SearchCriteria}, a thin layer over the generated builders.
 *
 * <p>A criteria is an OR of branches ({@link SearchCondition}); each branch is an AND of field
 * conditions. {@link #condition()} opens a branch, {@link Branch#where} (or its synonym {@link
 * Branch#and}) starts a predicate on a field, and every predicate returns the branch so the next
 * one can follow:
 *
 * <pre>{@code
 * SearchCriteria criteria = Criteria.builder()
 *     .condition()
 *         .where("type").equalTo(DocumentType.MOTION)
 *         .and("filed_at").between(Instant.parse("2024-01-01T00:00:00Z"), Instant.now())
 *         .and("tags").not().anyOf("archived", "draft")
 *     .or()
 *         .where("firm_id").equalTo("firm-2")
 *     .end()
 *     .fullText("merger agreement")
 *     .sortByScore(SortingOrder.DESC)
 *     .pageSize(20)
 *     .build();
 *
 * SearchCriteria simple = Criteria.where("status").equalTo("ACTIVE").build();
 * }</pre>
 *
 * <p>The generated classes remain the wire API: anything not covered here can be set on the
 * result with {@code toBuilder()}. Values follow {@link Values}.
 */
public final class Criteria {

  private final List<Branch> branches = new ArrayList<>();
  private final List<FieldCondition> everyBranch = new ArrayList<>();
  private final List<SortingCriteria> sorts = new ArrayList<>();
  private Pagination.Builder pagination;

  private Criteria() {}

  /**
   * Starts an empty criteria; built as is it matches every document.
   *
   * @return a new builder
   */
  public static Criteria builder() {
    return new Criteria();
  }

  /**
   * Starts a criteria with a single branch and a predicate on a field; shorthand for {@code
   * builder().condition().where(field)}.
   *
   * @param field the field path, such as {@code "parties.role"}
   * @return the predicate step
   */
  public static FieldStep where(String field) {
    return builder().condition().where(field);
  }

  /**
   * Opens a new branch. Branches are OR-ed: a document matches when every predicate of at least
   * one branch holds.
   *
   * @return the new branch
   */
  public Branch condition() {
    Branch branch = new Branch(this);
    branches.add(branch);
    return branch;
  }

  /**
   * Requires a full-text match across every full-text field of the index, in addition to every
   * branch (or on its own when there are none).
   *
   * @param query the text to search for
   * @return this builder
   */
  public Criteria fullText(String query) {
    everyBranch.add(fullTextCondition("", query, FullTextOptions.getDefaultInstance()));
    return this;
  }

  /**
   * Adds a sort by a field value; text fields sort by their keyword sub-field.
   *
   * @param field the field path
   * @param order the direction
   * @return this builder
   */
  public Criteria sortBy(String field, SortingOrder order) {
    sorts.add(SortingCriteria.newBuilder().setFieldSort(FieldSort.newBuilder()
        .setFieldName(Objects.requireNonNull(field, "field")).setOrder(order)).build());
    return this;
  }

  /**
   * Adds a sort by relevance score.
   *
   * @param order the direction
   * @return this builder
   */
  public Criteria sortByScore(SortingOrder order) {
    sorts.add(SortingCriteria.newBuilder().setScoreSort(ScoreSort.newBuilder().setOrder(order))
        .build());
    return this;
  }

  /**
   * Adds a sort by distance from a point.
   *
   * @param field a {@code geo_point} field
   * @param lat latitude of the point
   * @param lon longitude of the point
   * @param unit the unit of the sort values
   * @param order the direction
   * @return this builder
   */
  public Criteria sortByDistance(String field, double lat, double lon, DistanceUnit unit,
      SortingOrder order) {
    sorts.add(SortingCriteria.newBuilder().setGeoDistanceSort(GeoDistanceSort.newBuilder()
        .setFieldName(Objects.requireNonNull(field, "field"))
        .setPoint(LatLon.newBuilder().setLat(lat).setLon(lon))
        .setDistanceUnit(unit).setOrder(order)).build());
    return this;
  }

  /**
   * Sets from/size paging.
   *
   * @param size the page size; 0 means the engine default
   * @param from the offset of the first hit
   * @return this builder
   */
  public Criteria page(int size, int from) {
    pagination().setSize(size).setFrom(from);
    return this;
  }

  /**
   * Sets the page size.
   *
   * @param size the page size; 0 means the engine default
   * @return this builder
   */
  public Criteria pageSize(int size) {
    pagination().setSize(size);
    return this;
  }

  /**
   * Pages over a point in time with {@code search_after}.
   *
   * @param pitId the point-in-time id returned by the engine
   * @param keepAlive how long the engine keeps the point in time, such as {@code "1m"}, or empty
   * @param searchAfter the sort values of the last hit of the previous page, possibly empty;
   *     strings, numbers and booleans
   * @return this builder
   * @throws IllegalArgumentException when a sort value is of another type
   */
  public Criteria cursor(String pitId, String keepAlive, List<Object> searchAfter) {
    Pagination.Builder builder = pagination().setCursor(pitId).setKeepAlive(keepAlive)
        .clearSearchAfter();
    for (Object value : searchAfter) {
      builder.addSearchAfter(Values.primitive(value));
    }
    return this;
  }

  /**
   * Builds the criteria.
   *
   * @return the {@link SearchCriteria}
   */
  public SearchCriteria build() {
    SearchCriteria.Builder criteria = SearchCriteria.newBuilder();
    if (branches.isEmpty() && !everyBranch.isEmpty()) {
      criteria.addSearchCondition(SearchCondition.newBuilder().addAllFieldCondition(everyBranch));
    }
    for (Branch branch : branches) {
      criteria.addSearchCondition(SearchCondition.newBuilder()
          .addAllFieldCondition(branch.conditions).addAllFieldCondition(everyBranch));
    }
    criteria.addAllSortingCriteria(sorts);
    if (pagination != null) {
      criteria.setPagination(pagination);
    }
    return criteria.build();
  }

  private Pagination.Builder pagination() {
    if (pagination == null) {
      pagination = Pagination.newBuilder();
    }
    return pagination;
  }

  private static FieldCondition fullTextCondition(String field, String query,
      FullTextOptions options) {
    return FieldCondition.newBuilder().setField(field)
        .setOperation(SearchOperationType.FULL_TEXT)
        .setValue(Values.of(Objects.requireNonNull(query, "query")))
        .setFullText(options).build();
  }

  /** One branch of the criteria: an AND of field conditions. */
  public static final class Branch {

    private final Criteria parent;
    private final List<FieldCondition> conditions = new ArrayList<>();

    private Branch(Criteria parent) {
      this.parent = parent;
    }

    /**
     * Starts a predicate on a field.
     *
     * @param field the field path, such as {@code "parties.role"}
     * @return the predicate step
     */
    public FieldStep where(String field) {
      return new FieldStep(this, Objects.requireNonNull(field, "field"));
    }

    /**
     * Starts another predicate on a field of this branch; a synonym of {@link #where}.
     *
     * @param field the field path
     * @return the predicate step
     */
    public FieldStep and(String field) {
      return where(field);
    }

    /**
     * Adds a full-text match across every full-text field of the index to this branch.
     *
     * @param query the text to search for
     * @return this branch
     */
    public Branch fullText(String query) {
      return fullText(query, options -> {});
    }

    /**
     * Adds a full-text match to this branch, configured by the options builder; list fields with
     * {@code addFields} to restrict it.
     *
     * @param query the text to search for
     * @param options customizes the generated {@link FullTextOptions}
     * @return this branch
     */
    public Branch fullText(String query, Consumer<FullTextOptions.Builder> options) {
      FullTextOptions.Builder builder = FullTextOptions.newBuilder();
      options.accept(builder);
      conditions.add(fullTextCondition("", query, builder.build()));
      return this;
    }

    /**
     * Closes this branch and opens the next one, which is OR-ed with it.
     *
     * @return the new branch
     */
    public Branch or() {
      return parent.condition();
    }

    /**
     * Returns to the criteria to add sorting, paging or more branches.
     *
     * @return the criteria builder
     */
    public Criteria end() {
      return parent;
    }

    /**
     * Builds the whole criteria; shorthand for {@code end().build()}.
     *
     * @return the {@link SearchCriteria}
     */
    public SearchCriteria build() {
      return parent.build();
    }

    private Branch add(String field, SearchOperationType operation, FlatValue value,
        boolean negate) {
      FieldCondition.Builder condition = FieldCondition.newBuilder().setField(field)
          .setOperation(operation).setNegate(negate);
      if (value != null) {
        condition.setValue(value);
      }
      conditions.add(condition.build());
      return this;
    }
  }

  /** A predicate on one field, waiting for its operation. */
  public static final class FieldStep {

    private final Branch branch;
    private final String field;
    private boolean negate;

    private FieldStep(Branch branch, String field) {
      this.branch = branch;
      this.field = field;
    }

    /**
     * Negates the predicate that follows: it then excludes its matches.
     *
     * @return this step
     */
    public FieldStep not() {
      negate = true;
      return this;
    }

    /**
     * Matches an exact value ({@code EQUALS}). Null means the field is missing, a {@link
     * FlatValue} is used as is, enum constants compare by name.
     *
     * @param value a string, number, boolean, enum constant, {@link Instant}, {@code Timestamp},
     *     {@link FlatValue} or null
     * @return the branch
     * @throws IllegalArgumentException when the value is of another type
     */
    public Branch equalTo(Object value) {
      return add(SearchOperationType.EQUALS, Values.from(value));
    }

    /**
     * Matches documents without the field ({@code EQUALS} null).
     *
     * @return the branch
     */
    public Branch isNull() {
      return add(SearchOperationType.EQUALS, Values.nullValue());
    }

    /**
     * Matches documents that carry the field ({@code EXISTS}).
     *
     * @return the branch
     */
    public Branch exists() {
      return add(SearchOperationType.EXISTS, null);
    }

    /**
     * Runs an analyzed match on the field ({@code TEXT_MATCHES}).
     *
     * @param text the text to match
     * @return the branch
     */
    public Branch matches(String text) {
      return add(SearchOperationType.TEXT_MATCHES, Values.of(text));
    }

    /**
     * Matches a phrase on text, or a substring on keyword fields ({@code TEXT_CONTAINS}).
     *
     * @param text the phrase or substring
     * @return the branch
     */
    public Branch contains(String text) {
      return add(SearchOperationType.TEXT_CONTAINS, Values.of(text));
    }

    /**
     * Matches keyword values that start with a prefix ({@code TEXT_PREFIX}).
     *
     * @param prefix the prefix
     * @return the branch
     */
    public Branch startsWith(String prefix) {
      return add(SearchOperationType.TEXT_PREFIX, Values.of(prefix));
    }

    /**
     * Matches keyword values that end with a suffix ({@code TEXT_SUFFIX}); the compiler refuses
     * it unless leading wildcards are allowed.
     *
     * @param suffix the suffix
     * @return the branch
     */
    public Branch endsWith(String suffix) {
      return add(SearchOperationType.TEXT_SUFFIX, Values.of(suffix));
    }

    /**
     * Matches keyword values against a regular expression ({@code TEXT_REGEX}). The compiler
     * rejects it unless {@link QueryCompilerConfig#allowRegex()} is set, and rejects a pattern
     * that does not start with a literal character unless {@link
     * QueryCompilerConfig#allowLeadingWildcards()} is also set.
     *
     * @param pattern the Lucene regular expression
     * @return the branch
     */
    public Branch regex(String pattern) {
      return add(SearchOperationType.TEXT_REGEX, Values.of(pattern));
    }

    /**
     * Matches numbers or dates within inclusive bounds ({@code NUM_RANGE}); dates take epoch
     * milliseconds.
     *
     * @param min the inclusive lower bound
     * @param max the inclusive upper bound
     * @return the branch
     */
    public Branch between(double min, double max) {
      return add(SearchOperationType.NUM_RANGE, Values.between(min, max));
    }

    /**
     * Matches dates within inclusive bounds ({@code NUM_RANGE} in epoch milliseconds).
     *
     * @param from the inclusive start
     * @param to the inclusive end
     * @return the branch
     */
    public Branch between(Instant from, Instant to) {
      return add(SearchOperationType.NUM_RANGE, Values.between(from, to));
    }

    /**
     * Matches numbers or dates at or above the bound ({@code NUM_RANGE}, open on the
     * other side). Dates take epoch milliseconds.
     *
     * @param value the bound
     * @return the branch
     */
    public Branch atLeast(double value) {
      return add(SearchOperationType.NUM_RANGE, Values.atLeast(value));
    }

    /**
     * Matches numbers or dates strictly above the bound ({@code NUM_RANGE}, open on the
     * other side). Dates take epoch milliseconds.
     *
     * @param value the bound
     * @return the branch
     */
    public Branch greaterThan(double value) {
      return add(SearchOperationType.NUM_RANGE, Values.greaterThan(value));
    }

    /**
     * Matches numbers or dates at or below the bound ({@code NUM_RANGE}, open on the
     * other side). Dates take epoch milliseconds.
     *
     * @param value the bound
     * @return the branch
     */
    public Branch atMost(double value) {
      return add(SearchOperationType.NUM_RANGE, Values.atMost(value));
    }

    /**
     * Matches numbers or dates strictly below the bound ({@code NUM_RANGE}, open on the
     * other side). Dates take epoch milliseconds.
     *
     * @param value the bound
     * @return the branch
     */
    public Branch lessThan(double value) {
      return add(SearchOperationType.NUM_RANGE, Values.lessThan(value));
    }

    /**
     * Matches dates at or after the bound ({@code NUM_RANGE}, open on the
     * other side).
     *
     * @param value the bound
     * @return the branch
     */
    public Branch onOrAfter(Instant value) {
      return add(SearchOperationType.NUM_RANGE, Values.onOrAfter(value));
    }

    /**
     * Matches dates strictly after the bound ({@code NUM_RANGE}, open on the
     * other side).
     *
     * @param value the bound
     * @return the branch
     */
    public Branch after(Instant value) {
      return add(SearchOperationType.NUM_RANGE, Values.after(value));
    }

    /**
     * Matches dates at or before the bound ({@code NUM_RANGE}, open on the
     * other side).
     *
     * @param value the bound
     * @return the branch
     */
    public Branch onOrBefore(Instant value) {
      return add(SearchOperationType.NUM_RANGE, Values.onOrBefore(value));
    }

    /**
     * Matches dates strictly before the bound ({@code NUM_RANGE}, open on the
     * other side).
     *
     * @param value the bound
     * @return the branch
     */
    public Branch before(Instant value) {
      return add(SearchOperationType.NUM_RANGE, Values.before(value));
    }

    /**
     * Matches numbers or dates within bounds ({@code NUM_RANGE}).
     *
     * @param min the lower bound
     * @param minInclusive whether the lower bound matches
     * @param max the upper bound
     * @param maxInclusive whether the upper bound matches
     * @return the branch
     */
    public Branch range(double min, boolean minInclusive, double max, boolean maxInclusive) {
      return add(SearchOperationType.NUM_RANGE,
          Values.range(min, minInclusive, max, maxInclusive));
    }

    /**
     * Matches any of the values ({@code ANY_IN}).
     *
     * @param values strings, numbers, booleans or enum constants
     * @return the branch
     * @throws IllegalArgumentException when a value is of another type
     */
    public Branch anyOf(Object... values) {
      return add(SearchOperationType.ANY_IN, Values.list(values));
    }

    /**
     * Matches {@code geo_point} values within an arc distance of a point ({@code GEO_DISTANCE}).
     *
     * @param lat latitude of the centre
     * @param lon longitude of the centre
     * @param distance the radius with its unit, such as {@code "50km"}
     * @return the branch
     */
    public Branch withinDistance(double lat, double lon, String distance) {
      return add(SearchOperationType.GEO_DISTANCE, Values.distance(lat, lon, distance));
    }

    /**
     * Matches {@code geo_point} values inside a rectangle ({@code GEO_BOUNDING_BOX}).
     *
     * @param topLat latitude of the top edge
     * @param leftLon longitude of the left edge
     * @param bottomLat latitude of the bottom edge
     * @param rightLon longitude of the right edge
     * @return the branch
     */
    public Branch inBoundingBox(double topLat, double leftLon, double bottomLat,
        double rightLon) {
      return add(SearchOperationType.GEO_BOUNDING_BOX,
          Values.boundingBox(topLat, leftLon, bottomLat, rightLon));
    }

    /**
     * Matches {@code geo_point} values inside a WKT rectangle ({@code GEO_BOUNDING_BOX}).
     *
     * @param wkt a WKT {@code BBOX}
     * @return the branch
     */
    public Branch inBoundingBox(String wkt) {
      return add(SearchOperationType.GEO_BOUNDING_BOX, Values.boundingBox(wkt));
    }

    /**
     * Matches {@code geo_shape} values that intersect a shape ({@code GEO_INTERSECTS}).
     *
     * @param shape {@link Values#wkt}, {@link Values#geoJson} or {@link Values#latLon}
     * @return the branch
     */
    public Branch intersects(FlatValue shape) {
      return add(SearchOperationType.GEO_INTERSECTS, shape);
    }

    /**
     * Matches {@code geo_shape} values that contain a shape ({@code GEO_CONTAINS}).
     *
     * @param shape {@link Values#wkt}, {@link Values#geoJson} or {@link Values#latLon}
     * @return the branch
     */
    public Branch containsShape(FlatValue shape) {
      return add(SearchOperationType.GEO_CONTAINS, shape);
    }

    /**
     * Matches {@code geo_shape} values within a shape ({@code GEO_CONTAINED}).
     *
     * @param shape {@link Values#wkt} or {@link Values#geoJson}
     * @return the branch
     */
    public Branch withinShape(FlatValue shape) {
      return add(SearchOperationType.GEO_CONTAINED, shape);
    }

    /**
     * Runs a full-text match on this field only ({@code FULL_TEXT}).
     *
     * @param query the text to search for
     * @return the branch
     */
    public Branch fullText(String query) {
      return fullText(query, options -> {});
    }

    /**
     * Runs a full-text match on this field, configured by the options builder ({@code
     * FULL_TEXT}). Fields listed in the options take precedence over this field.
     *
     * @param query the text to search for
     * @param options customizes the generated {@link FullTextOptions}
     * @return the branch
     */
    public Branch fullText(String query, Consumer<FullTextOptions.Builder> options) {
      FullTextOptions.Builder builder = FullTextOptions.newBuilder();
      options.accept(builder);
      branch.conditions.add(fullTextCondition(field, query, builder.build()).toBuilder()
          .setNegate(negate).build());
      return branch;
    }

    private Branch add(SearchOperationType operation, FlatValue value) {
      return branch.add(field, operation, value, negate);
    }
  }
}
