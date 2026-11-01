package com.phatjam98.elasticsearch.utils;

import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.PropertyBuilders;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import com.google.common.base.CaseFormat;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import jakarta.inject.Singleton;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Utility class is used to create Elasticsearch Mappings from Protobuf Messages and to
 * create the IndexRequest needed by ElasticsearchService to create the new index.
 *
 * @param <T> Some generated Protobuf Class
 */
@Singleton
public class IndexUtils<T extends GeneratedMessageV3> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexUtils.class);
  private final Class<T> klass;

  /**
   * Provide the Protobuf Class to build the Index for.
   *
   * @param klass Generated Protobuf Class
   */
  public IndexUtils(Class<T> klass) {
    this.klass = klass;
  }

  public static <T extends GeneratedMessageV3> IndexUtils<T> getIndexUtils(Class<T> klass) {
    return new IndexUtils<>(klass);

  }

  public static <T extends GeneratedMessageV3> TypeMapping getTypeMapping(Class<T> klass) {
    return new IndexUtils<>(klass).generateTypeMapping();
  }

  /**
   * This method is used to generate a unique index suffix based on the Protobuf Message.  This
   * allows us to create a new index for each version of the Protobuf Message.
   *
   * @param <T>   Some generated Protobuf Class
   * @param klass Generated Protobuf Class
   * @return int hashcode of the properties
   */
  public static <T extends GeneratedMessageV3> int getIndexSuffix(Class<T> klass) {
    int mappingsHash = new IndexUtils<>(klass).generateTypeMapping().properties()
        .keySet().hashCode();
    return Math.abs(mappingsHash);
  }

  public static <T extends GeneratedMessageV3> String getIndexName(Class<T> klass) {
    return new IndexUtils<>(klass).extractIndexName();
  }

  public static <T extends GeneratedMessageV3> String getAlias(Class<T> klass) {
    return new IndexUtils<>(klass).getNormalizedName();
  }

  public String getAlias() {
    return getNormalizedName();
  }


  /**
   * This method is used to generate the TypeMapping for the Protobuf Message.
   *
   * @return TypeMapping
   */
  public TypeMapping generateTypeMapping() {
    var mappingBuilder = new TypeMapping.Builder();
    mappingBuilder.dynamic(DynamicMapping.Strict);

    Method getDescriptor = null;

    try {
      getDescriptor = klass.getMethod("getDescriptor");
    } catch (NoSuchMethodException e) {
      LOGGER.error("The method getDescriptor was not found on class {}", klass.getName(), e);
    }

    if (getDescriptor != null) {
      Descriptors.Descriptor descriptor = null;

      try {
        descriptor = (Descriptors.Descriptor) getDescriptor.invoke(null);
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOGGER.error("Error occurred trying to invoke getDescriptor method", e);
      }

      if (descriptor != null) {
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        Map<String, Property> properties = generateProperties(fields);
        mappingBuilder.properties(properties);
      }
    }

    return mappingBuilder.build();
  }

  public String extractIndexName() {
    return getNormalizedName() + "-" + getIndexSuffix(klass);
  }

  private String getNormalizedName() {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, klass.getSimpleName());
  }

  /**
   * This loops over the List of FileDescriptors to create the desired Mappings.  Some Protobuf
   * Messages include other Protobuf Messages.  In this case we default to creating Nested mappings
   * for that FieldDescriptor, for example a location object.  However, there are specific cases
   * where we do not want to create Nested Properties. BaseData is one such area we need to
   * flatten this without adding too much overhead to adding new protos and indicies.
   * <br/>
   * Repeated Fields are treated as Nested by default.
   *
   * @param descriptorList List of FieldDescriptors
   * @return Map of properties or completed mappings
   */
  private Map<String, Property> generateProperties(
      List<Descriptors.FieldDescriptor> descriptorList) {
    var properties = new HashMap<String, Property>();

    for (Descriptors.FieldDescriptor descriptor : descriptorList) {
      Property.Kind type = getEsType(descriptor);
      Property property = null;

      switch (type) {
        case Double:
          property = PropertyBuilders.double_(dnp -> dnp.index(true).coerce(true));
          break;
        case Float:
          property = PropertyBuilders.float_(fnp -> fnp.index(true).coerce(true));
          break;
        case Long:
          property = PropertyBuilders.long_(lnp -> lnp.index(true).coerce(true));
          break;
        case Integer:
          property = PropertyBuilders.integer(inp -> inp.index(true).coerce(true));
          break;
        case Boolean:
          property = PropertyBuilders.boolean_(bnp -> bnp.index(true));
          break;
        case Keyword:
          property = PropertyBuilders.keyword(knp -> knp.index(true));
          break;
        case Text:
          property = PropertyBuilders.text(tnp -> tnp.index(true));
          break;
        case GeoShape:
          property = PropertyBuilders.geoShape(gsp -> gsp.coerce(true));
          break;
        case Date:
          property = PropertyBuilders.date(dnp -> dnp.index(true)
              .format("strict_date_optional_time||epoch_second"));
          break;
        case Nested:
          var nestedProperties = generateProperties(descriptor.getMessageType().getFields());
          property = PropertyBuilders.nested(np -> np.properties(nestedProperties));
          break;
        default:
          LOGGER.error("Something fell through getting Elasticsearch Type.");
      }

      properties.put(descriptor.getName(), property);
    }

    return properties;
  }

  private Property.Kind getEsType(Descriptors.FieldDescriptor descriptor) {
    Property.Kind type = null;

    switch (descriptor.getType()) {
      case DOUBLE:
        type = Property.Kind.Double;
        break;
      case FLOAT:
        type = Property.Kind.Float;
        break;
      case INT64:
      case UINT64:
      case FIXED64:
      case SFIXED64:
      case SINT64:
        type = Property.Kind.Long;
        break;
      case INT32:
      case FIXED32:
      case UINT32:
      case SFIXED32:
      case SINT32:
        type = Property.Kind.Integer;
        break;
      case BOOL:
        type = Property.Kind.Boolean;
        break;
      case STRING:
        // This marks the wkt_centroids field as a geo_shape in the mappings.
        if (descriptor.getJsonName().toLowerCase(Locale.ROOT).contains("centroid")) {
          type = Property.Kind.GeoShape;
        } else if (descriptor.getJsonName().toLowerCase(Locale.ROOT).equals("id")) {
          type = Property.Kind.Text;
        } else {
          type = Property.Kind.Keyword;
        }
        break;
      case ENUM:
        type = Property.Kind.Keyword;
        break;
      case BYTES:
        type = Property.Kind.Text;
        break;
      case GROUP:
      case MESSAGE:
        String protoClass = descriptor.getMessageType().getFullName();

        if (protoClass.equals("com.phatjam98.protos.Data")) {
          type = Property.Kind.GeoShape;
        } else if (protoClass.equals("google.protobuf.Timestamp")) {
          type = Property.Kind.Date;
        } else {
          type = Property.Kind.Nested;
        }
        break;
      default:
        type = Property.Kind._Custom;
        LOGGER.error("Something fell through getting Elasticsearch Type.");
    }

    return type;
  }
}