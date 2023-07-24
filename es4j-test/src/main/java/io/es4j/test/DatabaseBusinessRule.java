package io.es4j.test;


import io.es4j.config.DatabaseConfiguration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseBusinessRule {
  /**
   * The class of the configuration
   * @return
   */
  Class<? extends DatabaseConfiguration> configurationClass();

  /**
   * The file where it is located
   * @return
   */
  String fileName();

  /**
   * the tenant the configuration should belong to
   * @return
   */
  String tenant() default "default";

  /**
   * The version of the file
   * @return
   */
  int version() default 0;

}

