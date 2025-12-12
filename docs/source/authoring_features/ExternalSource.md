# ExternalSource

ExternalSource is a powerful feature in Chronon that allows you to integrate real-time data from external services and APIs into your feature pipeline. Unlike standard GroupBys that compute features from batch or streaming data sources, ExternalSources fetch features on-demand from external systems during online serving.

## Table of Contents

1. [When to Use ExternalSource](#when-to-use-externalsource)
2. [Core Concepts](#core-concepts)
3. [Defining an ExternalSource](#defining-an-externalsource)
4. [Implementing ExternalSourceHandler](#implementing-externalsourcehandler)
5. [Registering Handlers](#registering-handlers)
6. [ContextualSource](#contextualsource)
7. [Feature Naming Conventions](#feature-naming-conventions)
8. [Offline Training with Bootstrap](#offline-training-with-bootstrap)
9. [Error Handling](#error-handling)
10. [Complete Example](#complete-example)
11. [Best Practices](#best-practices)

## When to Use ExternalSource

ExternalSource is ideal for scenarios where:

- **Real-time external data**: You need to fetch features from live services (e.g., user profile service, merchant service, inventory service)
- **Request context**: You need to pass request-specific metadata (event_id, session_id, experiment_id) for logging and tracking
- **Dynamic data**: The data is too fresh or changes too frequently to be materialized in batch/streaming pipelines
- **Third-party APIs**: You're integrating with external APIs or microservices
- **Point-in-time correctness**: You need the exact feature values that were available at request time, even if they change moments later

**Important**: ExternalSources are **online-only** by design. Chronon does not have a built-in mechanism to backfill external features. For offline training, you must use [bootstrap](#offline-training-with-bootstrap) to provide historical values.

## Core Concepts

### ExternalSource
An `ExternalSource` is a schema definition that specifies:
- **name**: Unique identifier for the external source
- **keySchema**: The fields needed to fetch data (e.g., user_id, merchant_id)
- **valueSchema**: The fields that will be returned (e.g., country, phone_verified, account_age)

### ExternalSourceHandler
An `ExternalSourceHandler` is the implementation that actually fetches the data. It's a user-provided class that:
- Receives a batch of fetch requests
- Makes calls to external services/APIs
- Returns feature values asynchronously via Futures
- Handles errors gracefully

### ExternalPart
An `ExternalPart` connects an ExternalSource to a Join, similar to how JoinPart connects a GroupBy to a Join. It supports:
- **keyMapping**: Remap keys from the left side to external source keys
- **prefix**: Add a prefix to distinguish features from different sources

## Defining an ExternalSource

### Python API

```python
from ai.chronon.api import ExternalSource, ExternalPart, Join
from ai.chronon.api.ttypes import StructType, StructField, StringType, LongType, IntType

# Define the external source schema
user_service_source = ExternalSource(
    metadata=MetaData(
        name="user_service",
        team="fraud_detection"
    ),
    # Keys needed to fetch from the service
    key_schema=StructType(
        name="user_service_keys",
        fields=[
            StructField("user_id", LongType)
        ]
    ),
    # Values returned by the service
    value_schema=StructType(
        name="user_service_values",
        fields=[
            StructField("country", StringType),
            StructField("phone_verified", IntType),
            StructField("account_age_days", IntType),
            StructField("risk_score", IntType)
        ]
    )
)

# Use in a Join
my_join = Join(
    left=...,
    right_parts=[...],
    online_external_parts=[
        ExternalPart(
            source=user_service_source,
            # Optional: remap keys from left to external source
            key_mapping={"user": "user_id"},
            # Optional: prefix to distinguish features
            prefix="us"
        )
    ]
)
```

### Scala API

```scala
import ai.chronon.api._
import ai.chronon.api.Builders._

val userServiceSource = ExternalSource(
  metadata = MetaData(
    name = "user_service",
    team = "fraud_detection"
  ),
  keySchema = StructType(
    name = "user_service_keys",
    fields = Array(StructField("user_id", LongType))
  ),
  valueSchema = StructType(
    name = "user_service_values",
    fields = Array(
      StructField("country", StringType),
      StructField("phone_verified", IntType),
      StructField("account_age_days", IntType),
      StructField("risk_score", IntType)
    )
  )
)

val join = Join(
  left = ...,
  joinParts = ...,
  externalParts = Seq(
    ExternalPart(
      userServiceSource,
      keyMapping = Map("user" -> "user_id"),
      prefix = "us"
    )
  )
)
```

## Implementing ExternalSourceHandler

### Scala Implementation

```scala
import ai.chronon.online.ExternalSourceHandler
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import scala.concurrent.Future
import scala.util.{Success, Failure}

class UserServiceHandler extends ExternalSourceHandler {

  override def fetch(requests: Seq[Request]): Future[Seq[Response]] = {
    // Batch requests by making parallel calls
    val responseFutures = requests.map { request =>
      // Extract user_id from keys
      val userId = request.keys("user_id").asInstanceOf[Long]

      // Call your external service
      callUserService(userId).map { userData =>
        Response(
          request = request,
          values = Success(Map(
            "country" -> userData.country,
            "phone_verified" -> userData.phoneVerified,
            "account_age_days" -> userData.accountAgeDays,
            "risk_score" -> userData.riskScore
          ))
        )
      }.recover { case ex: Exception =>
        // Handle errors - return failure
        Response(request, Failure(ex))
      }
    }

    Future.sequence(responseFutures)
  }

  private def callUserService(userId: Long): Future[UserData] = {
    // Your service call implementation
    // This could be HTTP, gRPC, or any other protocol
    ???
  }
}
```

### Java Implementation

For Java/Kotlin implementations, extend `JavaExternalSourceHandler` which provides automatic conversions between Scala and Java types:

```java
import ai.chronon.online.JavaExternalSourceHandler;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class UserServiceHandler extends JavaExternalSourceHandler {

    @Override
    public CompletableFuture<List<JavaResponse>> fetchJava(List<JavaRequest> requests) {
        // Process requests in parallel
        List<CompletableFuture<JavaResponse>> futures = requests.stream()
            .map(this::fetchSingleUser)
            .collect(Collectors.toList());

        // Combine all futures
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
    }

    private CompletableFuture<JavaResponse> fetchSingleUser(JavaRequest request) {
        Long userId = (Long) request.keys.get("user_id");

        return callUserService(userId)
            .thenApply(userData -> {
                Map<String, Object> values = new HashMap<>();
                values.put("country", userData.country);
                values.put("phone_verified", userData.phoneVerified);
                values.put("account_age_days", userData.accountAgeDays);
                values.put("risk_score", userData.riskScore);
                return JavaResponse.success(request, values);
            })
            .exceptionally(ex -> JavaResponse.failure(request, ex));
    }

    private CompletableFuture<UserData> callUserService(Long userId) {
        // Your service call implementation
        return null; // placeholder
    }
}
```

## Registering Handlers

Handlers must be registered in your `Api` implementation's `externalRegistry` method:

```scala
import ai.chronon.online.{Api, ExternalSourceRegistry, KVStore}

class MyApi(userConf: Map[String, String]) extends Api(userConf) {

  override def externalRegistry: ExternalSourceRegistry = {
    val registry = new ExternalSourceRegistry

    // Register your handlers by name (must match ExternalSource.metadata.name)
    registry.add("user_service", new UserServiceHandler())
    registry.add("merchant_service", new MerchantServiceHandler())
    registry.add("inventory_service", new InventoryServiceHandler())

    registry
  }

  // ... other Api methods
}
```

**Important**: The name used in `registry.add(name, handler)` must exactly match the `name` field in your ExternalSource's metadata.

## ContextualSource

`ContextualSource` is a special type of ExternalSource that passes request context through to the features without requiring an external service call. It's automatically handled by Chronon.

### Use Cases
- Pass request identifiers (event_id, request_id, session_id)
- Include experiment metadata (variant_id, experiment_name)
- Capture request metadata for logging and debugging

### Example

```python
from ai.chronon.api import ContextualSource, ExternalPart

# Define contextual fields
contextual_source = ContextualSource(
    fields=[
        ("event_id", StringType),
        ("session_id", StringType),
        ("experiment_variant", StringType)
    ]
)

# Add to Join
my_join = Join(
    left=...,
    right_parts=[...],
    online_external_parts=[
        ExternalPart(contextual_source)
    ],
    # These row_ids are used for bootstrap join matching
    row_ids=["event_id"]
)
```

### How It Works
The ContextualSource handler is pre-registered and simply echoes back the input keys as values. This allows you to:
1. Pass context through the feature pipeline
2. Log these values alongside computed features
3. Use them for joining bootstrap data offline

## Feature Naming Conventions

External source features follow a specific naming pattern:

```
ext_{prefix}_{source_name}_{field_name}
```

**Examples:**

| ExternalPart Config | Field Name | Output Feature Name |
|---------------------|------------|---------------------|
| `prefix=None, source="user_service"` | `country` | `ext_user_service_country` |
| `prefix="us", source="user_service"` | `country` | `ext_us_user_service_country` |
| `prefix=None, source="contextual"` | `event_id` | `ext_contextual_event_id` |

**Error Suffix**: When a fetch fails, Chronon creates an error feature with suffix `_exception`:
```
ext_user_service_exception  # Contains error message
```

## Offline Training with Bootstrap

Since ExternalSources only work online, you must provide historical values for offline training using [Bootstrap](Bootstrap.md).

### Strategy 1: Log-based Bootstrap

Use logged online requests to populate training data:

```python
v1 = Join(
    left=...,
    right_parts=[...],
    online_external_parts=[
        ExternalPart(ContextualSource(fields=[("event_id", StringType)])),
        ExternalPart(user_service_source)
    ],
    row_ids=["event_id"],
    # Automatically use logged values for bootstrap
    bootstrap_from_log=True
)
```

**How it works:**
1. Online requests are logged with all feature values (including external features)
2. The logged table is joined back to training data using `row_ids`
3. Historical external feature values are carried over from logs

### Strategy 2: Custom Bootstrap Table

Provide a custom backfill table with historical external feature values:

```python
v1 = Join(
    left=...,
    right_parts=[...],
    online_external_parts=[
        ExternalPart(user_service_source, prefix="us")
    ],
    bootstrap_parts=[
        BootstrapPart(
            table="my_namespace.historical_user_features",
            query=Query(
                selects={
                    # Map historical columns to Chronon feature names
                    "ext_us_user_service_country": "user_country",
                    "ext_us_user_service_phone_verified": "phone_verified_flag",
                    "ext_us_user_service_account_age_days": "account_age",
                    "ext_us_user_service_risk_score": "risk_score"
                },
                start_partition="2023-01-01",
                end_partition="2023-12-31"
            )
        )
    ]
)
```

**Important**: Column names in the bootstrap table must exactly match the external feature naming convention.

### Strategy 3: Hybrid Approach

Combine logged data for recent periods with custom backfill for historical periods:

```python
v1 = Join(
    left=driver_table_with_union_history,  # Union of old and new drivers
    online_external_parts=[...],
    # Bootstrap from log for recent data
    bootstrap_from_log=True,
    # Also include custom backfill for older data
    bootstrap_parts=[
        BootstrapPart(
            table="my_namespace.historical_external_features",
            query=Query(
                # Only use for dates before logging started
                wheres=["ds < '2023-06-01'"],
                selects={...}
            )
        )
    ]
)
```

## Error Handling

### Request-level Failures

When an individual request fails, Chronon creates an error feature:

```scala
class UserServiceHandler extends ExternalSourceHandler {
  override def fetch(requests: Seq[Request]): Future[Seq[Response]] = {
    Future.sequence(requests.map { request =>
      callUserService(request).recover {
        case ex: TimeoutException =>
          // Return failure - Chronon will create ext_user_service_exception
          Response(request, Failure(ex))
        case ex: NotFoundException =>
          // Return null values instead of error
          Response(request, Success(Map(
            "country" -> null,
            "phone_verified" -> null,
            "account_age_days" -> null
          )))
      }
    })
  }
}
```

### Handler Registration Failures

If a handler is not registered, Chronon returns a failure for all requests to that source:

```
IllegalArgumentException: user_service is not registered among handlers: [merchant_service, inventory_service]
```

### Best Practices

1. **Timeouts**: Always set reasonable timeouts on external calls
2. **Fallbacks**: Consider returning null values for non-critical failures
3. **Monitoring**: Log failures for debugging and alerting
4. **Batching**: Batch multiple requests to external services when possible
5. **Caching**: Consider caching at the handler level for frequently accessed data

## Complete Example

Here's a complete end-to-end example for a fraud detection use case:

### 1. Define ExternalSources

```python
# joins/fraud_detection/payment_fraud.py
from ai.chronon.api import *

# User service features
user_service = ExternalSource(
    metadata=MetaData(name="user_service", team="fraud"),
    key_schema=StructType("keys", [StructField("user_id", LongType)]),
    value_schema=StructType("values", [
        StructField("country", StringType),
        StructField("phone_verified", IntType),
        StructField("account_age_days", IntType)
    ])
)

# Merchant service features
merchant_service = ExternalSource(
    metadata=MetaData(name="merchant_service", team="fraud"),
    key_schema=StructType("keys", [StructField("merchant_id", LongType)]),
    value_schema=StructType("values", [
        StructField("merchant_category", StringType),
        StructField("merchant_risk_tier", IntType)
    ])
)

# Contextual features
contextual = ContextualSource(
    fields=[
        ("event_id", StringType),
        ("device_fingerprint", StringType)
    ]
)

# Define the join
v1 = Join(
    metadata=MetaData(
        name="fraud.payment_fraud_features",
        team="fraud",
        online=True
    ),
    left=Source.events(
        query=Query(
            selects={"user_id": "user_id", "merchant_id": "merchant_id"},
            time_column="ts"
        ),
        table="fraud.payment_events"
    ),
    right_parts=[
        JoinPart(group_by=payment_history.v1),
        JoinPart(group_by=user_profile.v1)
    ],
    online_external_parts=[
        ExternalPart(user_service, prefix="usr"),
        ExternalPart(merchant_service, prefix="merch"),
        ExternalPart(contextual)
    ],
    row_ids=["event_id"],
    bootstrap_from_log=True
)
```

### 2. Implement Handlers

```scala
// In your online service
class UserServiceHandler(httpClient: HttpClient) extends ExternalSourceHandler {

  override def fetch(requests: Seq[Request]): Future[Seq[Response]] = {
    val userIds = requests.map(_.keys("user_id").asInstanceOf[Long])

    // Batch call to user service
    httpClient.batchGetUsers(userIds).map { userData =>
      requests.map { request =>
        val userId = request.keys("user_id").asInstanceOf[Long]
        userData.get(userId) match {
          case Some(user) =>
            Response(request, Success(Map(
              "country" -> user.country,
              "phone_verified" -> (if (user.phoneVerified) 1 else 0),
              "account_age_days" -> user.accountAgeDays
            )))
          case None =>
            // User not found - return nulls
            Response(request, Success(Map(
              "country" -> null,
              "phone_verified" -> null,
              "account_age_days" -> null
            )))
        }
      }
    }.recover { case ex =>
      // Service error - return failures
      requests.map(req => Response(req, Failure(ex)))
    }
  }
}

class MerchantServiceHandler(httpClient: HttpClient) extends ExternalSourceHandler {
  // Similar implementation for merchant service
  override def fetch(requests: Seq[Request]): Future[Seq[Response]] = {
    // Implementation details...
    ???
  }
}
```

### 3. Register Handlers

```scala
class FraudApi(conf: Map[String, String]) extends Api(conf) {

  lazy val httpClient = new HttpClient(conf)

  override def externalRegistry: ExternalSourceRegistry = {
    val registry = new ExternalSourceRegistry
    registry.add("user_service", new UserServiceHandler(httpClient))
    registry.add("merchant_service", new MerchantServiceHandler(httpClient))
    // contextual is automatically registered
    registry
  }

  // ... other Api methods
}
```

### 4. Make Online Requests

```scala
val fetcher = api.buildFetcher()

val request = Request(
  name = "fraud.payment_fraud_features",
  keys = Map(
    "user_id" -> 12345L,
    "merchant_id" -> 67890L,
    "event_id" -> "evt_abc123",
    "device_fingerprint" -> "fp_xyz789"
  )
)

val responseFuture = fetcher.fetchJoin(Seq(request))
val responses = Await.result(responseFuture, Duration(5, SECONDS))

// Response contains all features:
// - GroupBy features from payment_history and user_profile
// - External features: ext_usr_user_service_country, ext_usr_user_service_phone_verified, etc.
// - Merchant features: ext_merch_merchant_service_merchant_category, etc.
// - Contextual: ext_contextual_event_id, ext_contextual_device_fingerprint
```

## Best Practices

### 1. Performance

- **Batch requests**: Handler receives multiple requests at once - batch external calls when possible
- **Async operations**: Use non-blocking async calls to external services
- **Timeouts**: Set aggressive timeouts (typically 50-200ms) to avoid blocking the serving path
- **Caching**: Consider request-level caching for frequently accessed, slowly-changing data

### 2. Reliability

- **Graceful degradation**: Return null values for non-critical features on failure
- **Circuit breakers**: Implement circuit breakers for flaky external services
- **Monitoring**: Instrument handlers with metrics (latency, error rates, cache hit rates)
- **Fallbacks**: Have fallback strategies when services are unavailable

### 3. Schema Management

- **Versioning**: Include version in ExternalSource names when schemas change
- **Backward compatibility**: Add new fields instead of modifying existing ones
- **Documentation**: Document the SLA and data freshness guarantees of each external source

### 4. Testing

- **Mock handlers**: Create mock implementations for testing
- **Load testing**: Test handler performance under high request volumes
- **Failure scenarios**: Test how your model behaves when external features are null/failed

### 5. Cost Management

- **Rate limiting**: Be aware of rate limits and costs of external APIs
- **Selective fetching**: Only fetch external features for high-value requests if possible
- **Caching**: Cache expensive-to-fetch features when appropriate

### 6. Offline Training

- **Plan for bootstrap**: Design your logging and bootstrap strategy upfront
- **Validate coverage**: Ensure bootstrap data covers your training date range
- **Monitor logging**: Alert on logging failures to avoid training data gaps
- **Column naming**: Double-check that bootstrap column names match the external feature naming convention exactly

## Related Documentation

- [Join](Join.md) - Core Join concepts and configuration
- [Bootstrap](Bootstrap.md) - Detailed bootstrap patterns for training data
- [Online Integration](../setup/Online_Integration.md) - Setting up online serving infrastructure
- [Serve](../test_deploy_serve/Serve.md) - Deploying and serving features online
