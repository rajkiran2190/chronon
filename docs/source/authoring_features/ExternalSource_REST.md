# REST and GraphQL ExternalSources

Chronon provides declarative REST and GraphQL ExternalSources that allow you to integrate external APIs without writing custom handler code. Simply define your API endpoint configuration, and Chronon handles the HTTP requests, authentication, retries, and response parsing automatically.

## Overview

Instead of implementing a custom `ExternalSourceHandler` in Scala or Java for each API integration, you can define a `RestExternalSource` or `GraphQLExternalSource` directly in your Python configuration with:

- URL endpoint and HTTP method
- Authentication configuration
- Request/response mapping
- Timeout, retry, and rate limiting settings

Chronon's generic handler takes care of the rest.

## Benefits

### 1. No Code Required
Define API integrations entirely in Python configuration - no Scala/Java handler implementation needed.

### 2. Built-in Best Practices
- **Automatic retries** with exponential backoff
- **Configurable timeouts** to prevent hanging requests
- **Rate limiting** to respect API quotas
- **Authentication** support (Bearer tokens, API keys, OAuth2, Basic Auth)
- **Batch optimization** for APIs that support batch requests

### 3. Faster Development
Go from idea to production in minutes instead of hours:
- **Before**: Define schema → Implement handler → Deploy code → Test
- **After**: Define schema + endpoint config → Test

### 4. Easier Maintenance
- Update endpoints, auth, or mappings without code changes
- Environment variable support for secrets
- All configuration versioned with your feature definitions

### 5. Consistent Patterns
Standardized approach across all REST/GraphQL integrations makes onboarding and debugging easier.

## REST ExternalSource

### Simple GET Request

```python
from ai.chronon.api import RestExternalSource, RestEndpoint, AuthConfig, AuthType, MetaData
from ai.chronon.api.ttypes import StructType, StructField, StringType, LongType, IntType, BooleanType, DoubleType

user_service = RestExternalSource(
    metadata=MetaData(name="user_service", team="fraud"),

    # Define keys needed to make the request
    key_schema=StructType("keys", [
        StructField("user_id", LongType)
    ]),

    # Define values returned from the API
    value_schema=StructType("values", [
        StructField("country", StringType),
        StructField("phone_verified", BooleanType),
        StructField("account_age_days", IntType),
        StructField("risk_score", DoubleType)
    ]),

    # Configure the REST endpoint
    endpoint=RestEndpoint(
        # URL with placeholder for user_id from key_schema
        url="https://user-service.company.com/v2/users/{user_id}",

        method=HttpMethod.GET,

        # Static headers
        headers={
            "Accept": "application/json",
            "X-Client-Id": "chronon-features"
        },

        # Authentication - token read from environment variable
        auth=AuthConfig(
            auth_type=AuthType.BEARER_TOKEN,
            token="${env.USER_SERVICE_TOKEN}"
        ),

        # Map JSON response fields to value_schema fields using JSONPath
        response_mapping={
            "country": "$.user.location.country_code",
            "phone_verified": "$.user.verification.phone_verified",
            "account_age_days": "$.user.account.age_in_days",
            "risk_score": "$.user.risk_assessment.score"
        },

        # Request timeout in milliseconds
        timeout_ms=3000,

        # Retry configuration
        retry=RetryConfig(
            max_retries=2,
            initial_backoff_ms=100,
            max_backoff_ms=1000
        )
    )
)

# Use in a Join like any other ExternalSource
my_join = Join(
    metadata=MetaData(name="fraud_model", team="fraud"),
    left=Source.events(...),
    right_parts=[...],
    online_external_parts=[
        ExternalPart(user_service, prefix="usr")
    ]
)
```

**Sample API Response:**
```json
{
  "user": {
    "location": {
      "country_code": "US"
    },
    "verification": {
      "phone_verified": true
    },
    "account": {
      "age_in_days": 487
    },
    "risk_assessment": {
      "score": 0.23
    }
  }
}
```

**Generated Features:**
- `ext_usr_user_service_country` = "US"
- `ext_usr_user_service_phone_verified` = true
- `ext_usr_user_service_account_age_days` = 487
- `ext_usr_user_service_risk_score` = 0.23

### POST Request with Body

```python
merchant_risk = RestExternalSource(
    metadata=MetaData(name="merchant_risk", team="fraud"),

    key_schema=StructType("keys", [
        StructField("merchant_id", LongType),
        StructField("transaction_amount", DoubleType)
    ]),

    value_schema=StructType("values", [
        StructField("risk_tier", StringType),
        StructField("max_transaction_limit", DoubleType),
        StructField("requires_review", BooleanType)
    ]),

    endpoint=RestEndpoint(
        url="https://risk-api.company.com/v1/assess",
        method=HttpMethod.POST,

        headers={
            "Content-Type": "application/json",
            "Accept": "application/json"
        },

        auth=AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="${env.RISK_API_KEY}",
            api_key_header="X-API-Key"  # Custom header name
        ),

        # Request body template with placeholders
        body_template="""{
            "merchant_id": "{merchant_id}",
            "transaction_amount": {transaction_amount},
            "include_history": true,
            "context": "real_time_scoring"
        }""",

        response_mapping={
            "risk_tier": "$.assessment.tier",
            "max_transaction_limit": "$.assessment.limits.max_transaction",
            "requires_review": "$.assessment.flags.manual_review_required"
        },

        timeout_ms=5000
    )
)
```

**Sample Request Body:**
```json
{
  "merchant_id": "789456",
  "transaction_amount": 1250.50,
  "include_history": true,
  "context": "real_time_scoring"
}
```

**Sample API Response:**
```json
{
  "assessment": {
    "tier": "medium",
    "limits": {
      "max_transaction": 5000.00
    },
    "flags": {
      "manual_review_required": false
    }
  }
}
```

### Batched Requests

For APIs that support batch requests, you can significantly reduce the number of HTTP calls:

```python
inventory_service = RestExternalSource(
    metadata=MetaData(name="inventory_batch", team="retail"),

    key_schema=StructType("keys", [
        StructField("item_id", LongType)
    ]),

    value_schema=StructType("values", [
        StructField("stock_count", IntType),
        StructField("warehouse_location", StringType),
        StructField("available", BooleanType)
    ]),

    endpoint=RestEndpoint(
        url="https://inventory.company.com/v1/items/batch",
        method=HttpMethod.POST,

        headers={"Content-Type": "application/json"},

        auth=AuthConfig(
            auth_type=AuthType.BEARER_TOKEN,
            token="${env.INVENTORY_TOKEN}"
        ),

        # Batch configuration
        batch_config=BatchConfig(
            enabled=True,
            max_batch_size=100,  # Maximum items per batch request

            # JSONPath where to inject item IDs in request
            batch_key_path="$.item_ids",

            # JSONPath to extract results array from response
            batch_result_path="$.items[*]",

            # Field in each result that maps back to the request key
            result_key_field="id"
        ),

        response_mapping={
            "stock_count": "$.stock_count",
            "warehouse_location": "$.warehouse.location_code",
            "available": "$.is_available"
        },

        timeout_ms=10000  # Longer timeout for batch requests
    )
)
```

**How it works:**
- Chronon receives 50 individual requests for different item_ids
- Instead of 50 HTTP calls, Chronon makes 1 batch request
- Batch request body: `{"item_ids": [101, 102, 103, ..., 150]}`
- Response is split back into individual responses

**Sample Batch Request:**
```json
{
  "item_ids": [101, 102, 103, 104, 105]
}
```

**Sample Batch Response:**
```json
{
  "items": [
    {"id": 101, "stock_count": 45, "warehouse": {"location_code": "WH-01"}, "is_available": true},
    {"id": 102, "stock_count": 0, "warehouse": {"location_code": "WH-02"}, "is_available": false},
    {"id": 103, "stock_count": 23, "warehouse": {"location_code": "WH-01"}, "is_available": true},
    {"id": 104, "stock_count": 12, "warehouse": {"location_code": "WH-03"}, "is_available": true},
    {"id": 105, "stock_count": 8, "warehouse": {"location_code": "WH-01"}, "is_available": true}
  ]
}
```

### Multiple Keys

```python
user_merchant_rel = RestExternalSource(
    metadata=MetaData(name="user_merchant_relationship", team="fraud"),

    # Multiple keys for the API call
    key_schema=StructType("keys", [
        StructField("user_id", LongType),
        StructField("merchant_id", LongType)
    ]),

    value_schema=StructType("values", [
        StructField("total_transactions", IntType),
        StructField("total_spend", DoubleType),
        StructField("first_transaction_date", StringType)
    ]),

    endpoint=RestEndpoint(
        # Both keys used in URL
        url="https://api.company.com/relationships/user/{user_id}/merchant/{merchant_id}",
        method=HttpMethod.GET,

        auth=AuthConfig(
            auth_type=AuthType.BEARER_TOKEN,
            token="${env.API_TOKEN}"
        ),

        response_mapping={
            "total_transactions": "$.relationship.transaction_count",
            "total_spend": "$.relationship.total_amount",
            "first_transaction_date": "$.relationship.first_seen"
        },

        timeout_ms=3000
    )
)
```

### Dynamic Headers from Keys

```python
tenant_specific_service = RestExternalSource(
    metadata=MetaData(name="tenant_service", team="platform"),

    key_schema=StructType("keys", [
        StructField("tenant_id", StringType),
        StructField("user_id", LongType)
    ]),

    value_schema=StructType("values", [
        StructField("subscription_tier", StringType),
        StructField("feature_flags", StringType)
    ]),

    endpoint=RestEndpoint(
        url="https://platform-api.company.com/users/{user_id}",
        method=HttpMethod.GET,

        # Static headers
        headers={
            "Accept": "application/json"
        },

        # Dynamic headers from request keys
        dynamic_headers={
            "X-Tenant-Id": "tenant_id"  # Maps tenant_id key to X-Tenant-Id header
        },

        auth=AuthConfig(
            auth_type=AuthType.BEARER_TOKEN,
            token="${env.PLATFORM_TOKEN}"
        ),

        response_mapping={
            "subscription_tier": "$.user.subscription.tier",
            "feature_flags": "$.user.features.enabled_flags"
        },

        timeout_ms=2000
    )
)
```

## GraphQL ExternalSource

```python
product_catalog = GraphQLExternalSource(
    metadata=MetaData(name="product_catalog", team="retail"),

    key_schema=StructType("keys", [
        StructField("product_id", StringType)
    ]),

    value_schema=StructType("values", [
        StructField("name", StringType),
        StructField("category", StringType),
        StructField("price", DoubleType),
        StructField("in_stock", BooleanType),
        StructField("rating", DoubleType)
    ]),

    endpoint=GraphQLEndpoint(
        url="https://catalog-api.company.com/graphql",

        # GraphQL query with variables
        query="""
            query GetProduct($productId: ID!) {
                product(id: $productId) {
                    name
                    category {
                        name
                    }
                    pricing {
                        current
                    }
                    inventory {
                        available
                    }
                    reviews {
                        averageRating
                    }
                }
            }
        """,

        # Map key_schema fields to GraphQL variables
        variable_mapping={
            "productId": "product_id"
        },

        # Map GraphQL response to value_schema fields
        response_mapping={
            "name": "$.data.product.name",
            "category": "$.data.product.category.name",
            "price": "$.data.product.pricing.current",
            "in_stock": "$.data.product.inventory.available",
            "rating": "$.data.product.reviews.averageRating"
        },

        auth=AuthConfig(
            auth_type=AuthType.BEARER_TOKEN,
            token="${env.CATALOG_TOKEN}"
        ),

        timeout_ms=3000
    )
)
```

**Sample GraphQL Response:**
```json
{
  "data": {
    "product": {
      "name": "Wireless Headphones",
      "category": {
        "name": "Electronics"
      },
      "pricing": {
        "current": 129.99
      },
      "inventory": {
        "available": true
      },
      "reviews": {
        "averageRating": 4.5
      }
    }
  }
}
```

## Authentication

### Bearer Token (Most Common)

```python
endpoint=RestEndpoint(
    url="https://api.company.com/resource/{id}",
    auth=AuthConfig(
        auth_type=AuthType.BEARER_TOKEN,
        token="${env.API_TOKEN}"  # Reads from environment variable
    )
)
```

**HTTP Request:**
```
GET /resource/123 HTTP/1.1
Host: api.company.com
Authorization: Bearer eyJhbGciOiJIUzI1NiIs...
```

### API Key

```python
endpoint=RestEndpoint(
    url="https://api.company.com/resource/{id}",
    auth=AuthConfig(
        auth_type=AuthType.API_KEY,
        api_key="${env.API_KEY}",
        api_key_header="X-API-Key"  # Optional, defaults to "X-API-Key"
    )
)
```

**HTTP Request:**
```
GET /resource/123 HTTP/1.1
Host: api.company.com
X-API-Key: ak_live_abcd1234...
```

### Basic Authentication

```python
endpoint=RestEndpoint(
    url="https://api.company.com/resource/{id}",
    auth=AuthConfig(
        auth_type=AuthType.BASIC_AUTH,
        username="${env.API_USERNAME}",
        password="${env.API_PASSWORD}"
    )
)
```

**HTTP Request:**
```
GET /resource/123 HTTP/1.1
Host: api.company.com
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
```

### OAuth2 Client Credentials

```python
endpoint=RestEndpoint(
    url="https://api.thirdparty.com/v3/data/{id}",
    auth=AuthConfig(
        auth_type=AuthType.OAUTH2,
        client_id="${env.OAUTH_CLIENT_ID}",
        client_secret="${env.OAUTH_CLIENT_SECRET}",
        token_url="https://auth.thirdparty.com/oauth/token",
        scopes=["data.read", "profile.read"]
    )
)
```

Chronon automatically:
- Fetches access token from `token_url` using client credentials flow
- Caches the token until expiration
- Refreshes token automatically when expired
- Adds `Authorization: Bearer <token>` header to requests

### No Authentication

```python
endpoint=RestEndpoint(
    url="https://public-api.company.com/data/{id}",
    auth=AuthConfig(auth_type=AuthType.NONE)
)
# Or simply omit the auth parameter
```

## JSONPath Response Mapping

JSONPath expressions extract values from JSON responses. Here are common patterns:

### Basic Field Access

```python
# Response: {"user": {"name": "Alice"}}
response_mapping={
    "name": "$.user.name"
}
```

### Nested Objects

```python
# Response: {"data": {"user": {"profile": {"country": "US"}}}}
response_mapping={
    "country": "$.data.user.profile.country"
}
```

### Array Access

```python
# Response: {"items": [{"id": 1, "value": 100}, {"id": 2, "value": 200}]}
response_mapping={
    "first_item_value": "$.items[0].value",
    "second_item_value": "$.items[1].value"
}
```

### Conditional Access

```python
# Response: {"primary_address": {"city": "NYC"}, "addresses": [{"type": "home", "city": "LA"}]}
response_mapping={
    # Get primary address city
    "city": "$.primary_address.city",

    # Get first address in array
    "backup_city": "$.addresses[0].city"
}
```

### Default Values

If a JSONPath doesn't match, Chronon returns `null` for that field:

```python
# Response: {"user": {"name": "Bob"}}  (no email field)
response_mapping={
    "name": "$.user.name",        # Returns "Bob"
    "email": "$.user.email"       # Returns null (field doesn't exist)
}
```

## Error Handling

### Request-Level Errors

When an API request fails, Chronon creates an error feature:

```python
user_service = RestExternalSource(
    metadata=MetaData(name="user_service"),
    # ... configuration ...
)

# If request fails, you get:
# - ext_user_service_country = null
# - ext_user_service_phone_verified = null
# - ext_user_service_exception = "Connection timeout after 3000ms"
```

### Retry Configuration

```python
endpoint=RestEndpoint(
    url="...",
    retry=RetryConfig(
        max_retries=3,                 # Total retry attempts
        initial_backoff_ms=100,        # First retry after 100ms
        max_backoff_ms=2000,           # Max backoff of 2 seconds
        backoff_multiplier=2.0,        # Exponential backoff (100, 200, 400, 800, ...)
        retryable_status_codes=[429, 500, 502, 503, 504]  # Which HTTP codes to retry
    )
)
```

**Retry behavior:**
- 1st failure: Wait 100ms, retry
- 2nd failure: Wait 200ms, retry
- 3rd failure: Wait 400ms, retry
- 4th failure: Return error to user

### Timeouts

```python
endpoint=RestEndpoint(
    url="...",
    timeout_ms=5000  # Fail if no response within 5 seconds
)
```

Set appropriate timeouts based on your serving SLA:
- **Critical path**: 50-200ms
- **Standard features**: 500-2000ms
- **Batch endpoints**: 5000-10000ms

## Rate Limiting

Protect external APIs from being overwhelmed:

```python
endpoint=RestEndpoint(
    url="...",
    rate_limit=RateLimitConfig(
        requests_per_second=100,    # Max 100 requests/second to this API
        concurrent_requests=10,      # Max 10 simultaneous requests
        burst_size=20               # Allow bursts up to 20 requests
    )
)
```

Chronon automatically queues requests that exceed the rate limit.

## Environment Variables

Use environment variables for secrets and configuration that varies by environment:

```python
endpoint=RestEndpoint(
    url="https://${env.API_HOST}/v1/users/{user_id}",

    auth=AuthConfig(
        auth_type=AuthType.BEARER_TOKEN,
        token="${env.USER_SERVICE_TOKEN}"
    ),

    headers={
        "X-Environment": "${env.ENVIRONMENT}"
    }
)
```

**Environment setup:**
```bash
export API_HOST="api.prod.company.com"
export USER_SERVICE_TOKEN="prod_token_abc123"
export ENVIRONMENT="production"
```

## Complete Example: Fraud Detection

```python
from ai.chronon.api import *

# 1. User service - GET request with bearer token
user_service = RestExternalSource(
    metadata=MetaData(name="user_service", team="fraud"),
    key_schema=StructType("keys", [StructField("user_id", LongType)]),
    value_schema=StructType("values", [
        StructField("country", StringType),
        StructField("phone_verified", BooleanType),
        StructField("account_age_days", IntType),
        StructField("total_purchases", IntType)
    ]),
    endpoint=RestEndpoint(
        url="https://user-api.company.com/v2/users/{user_id}",
        method=HttpMethod.GET,
        auth=AuthConfig(auth_type=AuthType.BEARER_TOKEN, token="${env.USER_TOKEN}"),
        response_mapping={
            "country": "$.user.location.country",
            "phone_verified": "$.user.verification.phone",
            "account_age_days": "$.user.account.age_days",
            "total_purchases": "$.user.stats.purchase_count"
        },
        timeout_ms=2000
    )
)

# 2. Device fingerprint service - POST request with API key
device_service = RestExternalSource(
    metadata=MetaData(name="device_fingerprint", team="fraud"),
    key_schema=StructType("keys", [StructField("device_id", StringType)]),
    value_schema=StructType("values", [
        StructField("risk_score", DoubleType),
        StructField("device_type", StringType),
        StructField("is_emulator", BooleanType)
    ]),
    endpoint=RestEndpoint(
        url="https://device-api.company.com/v1/analyze",
        method=HttpMethod.POST,
        auth=AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="${env.DEVICE_API_KEY}",
            api_key_header="X-API-Key"
        ),
        body_template='{"device_id": "{device_id}", "include_risk": true}',
        response_mapping={
            "risk_score": "$.analysis.risk_score",
            "device_type": "$.device.type",
            "is_emulator": "$.device.is_emulator"
        },
        timeout_ms=3000,
        retry=RetryConfig(max_retries=2)
    )
)

# 3. Third-party enrichment - OAuth2 with batching
enrichment_service = RestExternalSource(
    metadata=MetaData(name="enrichment", team="fraud"),
    key_schema=StructType("keys", [StructField("email_hash", StringType)]),
    value_schema=StructType("values", [
        StructField("income_bucket", StringType),
        StructField("age_range", StringType)
    ]),
    endpoint=RestEndpoint(
        url="https://api.thirdparty.com/v3/batch",
        method=HttpMethod.POST,
        auth=AuthConfig(
            auth_type=AuthType.OAUTH2,
            client_id="${env.ENRICHMENT_CLIENT_ID}",
            client_secret="${env.ENRICHMENT_SECRET}",
            token_url="https://auth.thirdparty.com/token",
            scopes=["enrichment.read"]
        ),
        batch_config=BatchConfig(
            enabled=True,
            max_batch_size=50,
            batch_key_path="$.email_hashes",
            batch_result_path="$.results[*]",
            result_key_field="email_hash"
        ),
        response_mapping={
            "income_bucket": "$.demographics.income_bucket",
            "age_range": "$.demographics.age_range"
        },
        timeout_ms=5000,
        rate_limit=RateLimitConfig(requests_per_second=50)
    )
)

# 4. Contextual request data
request_context = ContextualSource(
    fields=[
        ("event_id", StringType),
        ("session_id", StringType),
        ("ip_address", StringType)
    ]
)

# 5. Define the join
fraud_features = Join(
    metadata=MetaData(
        name="fraud.transaction_features",
        team="fraud",
        online=True
    ),
    left=Source.events(
        query=Query(
            selects={
                "user_id": "user_id",
                "device_id": "device_id",
                "email_hash": "email_hash"
            },
            time_column="ts"
        ),
        table="fraud.transactions"
    ),
    right_parts=[
        JoinPart(group_by=transaction_history.v1),
        JoinPart(group_by=user_profile.v1)
    ],
    online_external_parts=[
        ExternalPart(user_service, prefix="usr"),
        ExternalPart(device_service, prefix="dev"),
        ExternalPart(enrichment_service, prefix="enrich"),
        ExternalPart(request_context)
    ],
    row_ids=["event_id"],
    bootstrap_from_log=True
)
```

**Generated feature names:**
- User service: `ext_usr_user_service_country`, `ext_usr_user_service_phone_verified`, etc.
- Device service: `ext_dev_device_fingerprint_risk_score`, `ext_dev_device_fingerprint_device_type`, etc.
- Enrichment: `ext_enrich_enrichment_income_bucket`, `ext_enrich_enrichment_age_range`
- Context: `ext_contextual_event_id`, `ext_contextual_session_id`, `ext_contextual_ip_address`

## Best Practices

### 1. Set Appropriate Timeouts

Match timeouts to your serving SLA:
```python
# Critical features on hot path
timeout_ms=200

# Important but not critical
timeout_ms=1000

# Batch/background enrichment
timeout_ms=5000
```

### 2. Use Batching When Available

Batching can reduce API calls by 10-100x:
```python
# 100 individual requests = 100 API calls
# With batch_config(max_batch_size=100) = 1 API call
```

### 3. Implement Rate Limiting

Protect APIs and respect quotas:
```python
rate_limit=RateLimitConfig(
    requests_per_second=100,  # Based on your API quota
    concurrent_requests=10     # Based on API's connection limits
)
```

### 4. Use Environment Variables for Secrets

Never hardcode tokens:
```python
# Good
token="${env.API_TOKEN}"

# Bad
token="sk_live_abcd1234..."
```

### 5. Configure Retries Appropriately

```python
# For idempotent GET requests
retry=RetryConfig(max_retries=3)

# For non-idempotent POST/PUT (use with caution)
retry=RetryConfig(max_retries=0)
```

### 6. Plan for Bootstrap

External features are online-only. Plan your offline training strategy:

```python
fraud_features = Join(
    # ...
    online_external_parts=[ExternalPart(user_service)],
    row_ids=["event_id"],           # For joining logged data
    bootstrap_from_log=True          # Use logged values for training
)
```

### 7. Monitor and Alert

External APIs can fail. Monitor:
- Error rates: `ext_{source}_exception` features
- Latencies: Integration with your metrics system
- Rate limit hits: Configure alerts

### 8. Test Endpoint Configuration

Before deploying, test your configuration:
```bash
# Use Chronon CLI to validate endpoint config
chronon test-external-source joins/fraud/features.py:user_service
```

## Troubleshooting

### Feature Values Are Null

**Cause**: JSONPath mapping doesn't match response structure

**Solution**: Check the actual API response and adjust mapping:
```python
# Wrong: "$.user.country"
# Correct: "$.data.user.location.country"
response_mapping={
    "country": "$.data.user.location.country"
}
```

### Timeout Errors

**Cause**: API is slow or timeout too aggressive

**Solution**: Increase timeout or investigate API latency:
```python
timeout_ms=5000  # Increase from 1000ms
```

### Authentication Failures

**Cause**: Invalid or expired token

**Solution**: Verify environment variable:
```bash
echo $USER_SERVICE_TOKEN  # Check token is set
# Verify token works with curl
curl -H "Authorization: Bearer $USER_SERVICE_TOKEN" https://api.company.com/users/123
```

### Rate Limit Errors

**Cause**: Exceeding API quota

**Solution**: Add or adjust rate limiting:
```python
rate_limit=RateLimitConfig(
    requests_per_second=50  # Reduce from 100
)
```

### Batch Responses Don't Match Requests

**Cause**: Incorrect `result_key_field` mapping

**Solution**: Ensure result key field matches request key:
```python
# Request key: "item_id"
# Response: [{"id": 101, ...}, {"id": 102, ...}]

batch_config=BatchConfig(
    result_key_field="id"  # Must match "id" in response
)
```

## Migration from Custom Handlers

If you have existing custom `ExternalSourceHandler` implementations, you can migrate gradually:

### Before (Custom Handler)

```python
# Configuration
user_service = ExternalSource(
    metadata=MetaData(name="user_service"),
    key_schema=StructType("keys", [StructField("user_id", LongType)]),
    value_schema=StructType("values", [...])
)

# Separate Scala handler implementation (50+ lines)
class UserServiceHandler extends ExternalSourceHandler {
  // Custom HTTP code...
}
```

### After (REST ExternalSource)

```python
# Single configuration - no custom code
user_service = RestExternalSource(
    metadata=MetaData(name="user_service"),
    key_schema=StructType("keys", [StructField("user_id", LongType)]),
    value_schema=StructType("values", [...]),
    endpoint=RestEndpoint(
        url="https://user-api.company.com/v2/users/{user_id}",
        auth=AuthConfig(auth_type=AuthType.BEARER_TOKEN, token="${env.USER_TOKEN}"),
        response_mapping={...}
    )
)
```

**Benefits:**
- Eliminate 50+ lines of handler code
- All configuration in Python
- Built-in retry, timeout, rate limiting
- Easier to test and maintain

## Related Documentation

- [ExternalSource](ExternalSource.md) - Overview of ExternalSource and custom handlers
- [Join](Join.md) - Using ExternalSource in Joins
- [Bootstrap](Bootstrap.md) - Offline training with external features
- [Online Integration](../setup/Online_Integration.md) - Setting up online serving
