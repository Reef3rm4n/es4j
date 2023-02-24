package io.vertx.skeleton.evs.reflection;


import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mutiny.core.Vertx;
import io.vertx.skeleton.evs.Command;
import io.vertx.skeleton.evs.Behaviour;
import io.vertx.skeleton.evs.Entity;
import io.vertx.skeleton.evs.domain.behaviours.ChangeData1BehaviourEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
class ReflectionUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionUtilsTest.class);


  @Test
  void testReflection(Vertx vertx, VertxTestContext vertxTestContext) {
    final var classTuple = entityAggregateClass(ChangeData1BehaviourEntity.class);
    vertxTestContext.completeNow();
  }

  public Tuple2<Class<? extends Entity>, Class<? extends Command>> entityAggregateClass(Class<? extends Behaviour<? extends Entity, ? extends Command>> behaviour) {
    Type[] genericInterfaces = behaviour.getGenericInterfaces();
    if (genericInterfaces.length > 1) {
      throw new IllegalArgumentException("Behaviours cannot implement more than one interface -> " + behaviour.getName());
    } else if (genericInterfaces.length == 0) {
      throw new IllegalArgumentException("Behaviours should implement BehaviourCommand interface -> " + behaviour.getName());
    }
    final var genericInterface = genericInterfaces[0];
    if (genericInterface instanceof ParameterizedType parameterizedType) {
      Type[] genericTypes = parameterizedType.getActualTypeArguments();
      LOGGER.info("Types -> " + Arrays.stream(genericTypes).map(t -> t.getTypeName()).toList());
      final Class<? extends Entity> entityClass;
      Class<? extends Command> commandClass;
      try {
        entityClass = (Class<? extends Entity>) Class.forName(genericTypes[0].getTypeName());
        commandClass = (Class<? extends Command>) Class.forName(genericTypes[1].getTypeName());
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Unable to get behaviour generic types -> ", e);
      }
      LOGGER.info("Entity class -> " + entityClass.getName());
      LOGGER.info("Command class -> " + commandClass.getName());
      return Tuple2.of(entityClass, commandClass);
    } else {
      throw new IllegalArgumentException("Invalid genericInterface -> " + genericInterface.getClass());
    }
  }



  @Test
  void testConsistentHashing2(Vertx vertx, VertxTestContext vertxTestContext) {

  }

}
