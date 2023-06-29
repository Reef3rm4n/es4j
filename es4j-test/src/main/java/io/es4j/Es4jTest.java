package io.es4j;


import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(Es4jExtension.class)
public @interface Es4jTest {

  Class<? extends Aggregate> aggregate();

  boolean infrastructure() default true;

  boolean cache() default true;

  boolean secondaryEventStore() default false;

  String host() default "localhost";

  int port() default 8080;


}
