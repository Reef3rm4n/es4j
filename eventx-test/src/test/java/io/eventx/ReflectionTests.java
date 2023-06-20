package io.eventx;


import io.eventx.behaviours.ChangeBehaviour;
import io.smallrye.mutiny.tuples.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReflectionTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionTests.class);


  @Test
  void testReflection() {
    final var classTuple = entityAggregateClass(ChangeBehaviour.class);
  }

  public Tuple2<Class<? extends Aggregate>, Class<? extends Command>> entityAggregateClass(Class<? extends Behaviour<? extends Aggregate, ? extends Command>> behaviour) {
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
      final Class<? extends Aggregate> entityClass;
      Class<? extends Command> commandClass;
      try {
        entityClass = (Class<? extends Aggregate>) Class.forName(genericTypes[0].getTypeName());
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


}
