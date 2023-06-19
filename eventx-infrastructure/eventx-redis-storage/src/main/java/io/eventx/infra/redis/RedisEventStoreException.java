package io.eventx.infra.redis;

import io.eventx.core.exceptions.EventxException;
import io.eventx.core.objects.ErrorSource;
import io.eventx.core.objects.EventxError;
import io.eventx.core.objects.EventxErrorBuilder;

public class RedisEventStoreException extends EventxException {

  public RedisEventStoreException(EventxError eventxError) {
    super(eventxError);
  }

  public RedisEventStoreException(String redisError) {
    super(EventxErrorBuilder.builder()
      .errorSource(ErrorSource.INFRASTRUCTURE)
      .cause(redisError)
      .build());
  }
}
