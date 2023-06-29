package io.es4j.infra.redis;

import io.es4j.core.exceptions.Es4jException;
import io.es4j.core.objects.ErrorSource;
import io.es4j.core.objects.Es4jError;
import io.es4j.core.objects.Es4jErrorBuilder;

public class RedisEventStoreException extends Es4jException {

  public RedisEventStoreException(Es4jError es4jError) {
    super(es4jError);
  }

  public RedisEventStoreException(String redisError) {
    super(Es4jErrorBuilder.builder()
      .errorSource(ErrorSource.INFRASTRUCTURE)
      .cause(redisError)
      .build());
  }
}
