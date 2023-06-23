package io.eventx;


import io.eventx.config.DatabaseConfiguration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseBusinessRule {
  Class<? extends DatabaseConfiguration> configurationClass();
  String fileName();
  String tenant() default "default";
  int version() default 0;

}

