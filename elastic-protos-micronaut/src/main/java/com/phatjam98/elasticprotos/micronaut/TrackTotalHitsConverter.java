package com.phatjam98.elasticprotos.micronaut;

import com.phatjam98.elasticprotos.query.TrackTotalHits;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.MutableConversionService;
import io.micronaut.core.convert.TypeConverter;
import io.micronaut.core.convert.TypeConverterRegistrar;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Converts {@code elastic-protos.query.track-total-hits} values to {@link TrackTotalHits}: {@code
 * true}, {@code false}, {@code default} or a non-negative number, as {@link
 * TrackTotalHits#parse(CharSequence)} reads them. Text, booleans and numbers are accepted, so that
 * both {@code track-total-hits: false} in YAML and {@code "false"} from an environment variable
 * bind. An invalid value is rejected, which fails the configuration binding.
 */
@Singleton
public class TrackTotalHitsConverter
    implements TypeConverter<CharSequence, TrackTotalHits>, TypeConverterRegistrar {

  /** Creates the converter. */
  public TrackTotalHitsConverter() {}

  @Override
  public Optional<TrackTotalHits> convert(CharSequence value, Class<TrackTotalHits> targetType,
      ConversionContext context) {
    return parse(value, context);
  }

  @Override
  public void register(MutableConversionService conversionService) {
    conversionService.addConverter(CharSequence.class, TrackTotalHits.class, this);
    conversionService.addConverter(Boolean.class, TrackTotalHits.class,
        (TypeConverter<Boolean, TrackTotalHits>) (value, type, context) ->
            parse(value.toString(), context));
    conversionService.addConverter(Number.class, TrackTotalHits.class,
        (TypeConverter<Number, TrackTotalHits>) (value, type, context) ->
            parse(value.toString(), context));
  }

  private static Optional<TrackTotalHits> parse(CharSequence value, ConversionContext context) {
    try {
      return Optional.of(TrackTotalHits.parse(value));
    } catch (IllegalArgumentException e) {
      context.reject(value, e);
      return Optional.empty();
    }
  }
}
