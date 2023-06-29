# ES4J - Vert.x

# Aggregate Interface

The `Aggregate` interface is a fundamental part of our event sourcing framework. It manages the state of an aggregate, which represents an entity in our domain.

## Methods

The `Aggregate` interface defines the following methods:

- **`aggregateId()`**: Returns the unique identifier of the aggregate instance.

- **`tenantID()`**: Returns the tenant ID associated with the aggregate. By default, it returns "default". However, this can be overridden in your implementation to return a specific tenant ID.

- **`schemaVersion()`**: Returns the current schema version of the aggregate. The default implementation returns 0. However, it's expected that this will be overridden with the correct version in your implementations.

- **`snapshotEvery()`**: Returns an `Optional<Integer>` that indicates how often a snapshot of the aggregate's state should be taken. The default implementation returns an empty Optional, meaning no snapshots are taken. However, this method can be overridden to specify a snapshotting strategy.

- **`transformSnapshot(int schemaVersion, JsonObject snapshot)`**: This method transforms a snapshot from a specific schema version to the current version. The default implementation throws an `UnknownEvent` exception. It's expected that this will be overridden in your implementations to provide the correct transformation logic.

## Implementation Example

Here's an example of how this interface could be implemented:

```java
public record Account(String id, String owner, BigDecimal balance) implements Aggregate {

  @Override
  public String aggregateId() {
    return id();
  }

  @Override
  public String tenant() {
    return owner();
  }

  @Override
  public int schemaVersion() {
    return 2;
  }

  @Override
  public Optional<Integer> snapshotEvery() {
    // Create a snapshot every 100 events
    return Optional.of(100);
  }

  @Override
  public Aggregate transformSnapshot(int schemaVersion, JsonObject snapshot) {
    return switch (schemaVersion) {
      case 1 ->
        // transform the aggregate
         new Account(
                snapshot.get("field1"),
                snapshot.get("field2"),
                snapshot.get("field3")
        );
      default ->
        throw new UnknownEvent(new Es4jError(
          ErrorSource.LOGIC,
          EventBehaviour.class.getName(),
          "missing schema versionTo " + schemaVersion,
          "could not transform event",
          "aggregate.event.transform",
          500
        ));
    };
  }
}
```

In this example, `Account` implements the `Aggregate` interface. It provides specific implementations for `tenantID()`, `schemaVersion()`, `snapshotEvery()`, and `transformSnapshot()`.

For more examples and detailed usage of the `Aggregate` interface, please navigate to the specific repositories.


## `CommandBehaviour` Interface

The `CommandBehaviour` interface is part of our event sourcing framework, and it manages the commands for a given aggregate.

### Example

Consider a banking application where we have an `Account` aggregate and a `DepositCommand`. We might define our command behavior like so:

```java
public class DepositBehaviour implements Behaviour<Account, DepositCommand> {

    @Override
    public List<Event> process(Account account, DepositCommand command) {
        List<Event> events = new ArrayList<>();

        // validation
        if (command.getAmount() <= 0) {
            events.add(new InvalidDepositEvent(command.getAmount()));
        } else {
            events.add(new MoneyDepositedEvent(account.getId(), command.getAmount()));
        }

        return events;
    }
}
```

In the above example, the `DepositBehaviour` processes a `DepositCommand` for the `Account` aggregate. If the deposit amount is invalid (i.e., less than or equal to zero), it produces an `InvalidDepositEvent`. If the deposit is valid, it generates a `MoneyDepositedEvent`.


# EventBehaviour Interface

The `EventBehaviour` interface plays a vital role in event.x, managing how events apply to aggregates.

## Type Parameters

The interface is parameterized with two types:

1. **`T extends Aggregate`**: T signifies the aggregate root, which is an aggregate in the Domain Driven Design (DDD) context. Essentially, an aggregate is a cluster of associated objects that are treated as a unit for data changes.

2. **`E extends Event`**: E represents the event to be applied to the aggregate. In event sourcing, events generally signify something that has occurred.

## Methods

The `EventBehaviour` interface specifies four methods, each serving a unique purpose.

- **`apply(T aggregateState, E event)`**: This method applies an event to an aggregate's state and gives back the updated state of the aggregate.

- **`tenantId()`**: This method provides the tenant ID associated with the event. It returns "default" by default, but it can be overridden to facilitate specific tenant identification.

- **`currentSchemaVersion()`**: This method returns the current schema version of the event. It returns 0 by default.

- **`transformFrom(int schemaVersion, JsonObject event)`**: This method transforms an event from a specific schema version to the current version. By default, it throws an `UnknownEvent` exception, which should be overridden to provide the correct transformation logic in your implementations.

## Implementation Example

Here's an example of how this interface could be implemented:

```java
public class AccountCreatedBehaviour implements Aggregator<Account, AccountCreatedEvent> {

    @Override
    public Account apply(Account aggregateState, AccountCreatedEvent event) {
        aggregateState.setId(event.getId());
        aggregateState.setBalance(event.getInitialBalance());
        aggregateState.setOwner(event.getOwner());
        return aggregateState;
    }

    @Override
    public int currentSchemaVersion() {
        return 1;
    }
}
```

In this example, `AccountCreatedBehaviour` implements the `EventBehaviour` interface for `Account` aggregate and `AccountCreatedEvent`. The `apply()` method is implemented to apply the `AccountCreatedEvent` to the `Account` state. Also, the `currentSchemaVersion()` method is overridden to return the current schema version as 1.

For more examples and detailed usage of the `EventBehaviour` interface, please navigate to the specific repositories.


Thank you for visiting my GitHub profile, and don't hesitate to reach out if you have any questions or comments!
