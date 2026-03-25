// =============================================================================
// MongoDB Initialization Script for kshark Integration Testbed
// =============================================================================
//
// This script runs on first startup of the MongoDB container.
// It is executed as the root user (MONGO_INITDB_ROOT_USERNAME).
//
// It creates:
//   1. The 'testdb' database
//   2. A 'testuser' with readWrite access to 'testdb'
//   3. The 'events' collection with a validator
//   4. Seed data for integration tests

// Switch to the target database
db = db.getSiblingDB("testdb");

// Create a user with readWrite privileges on testdb.
// Note: The root user is already created by MONGO_INITDB_ROOT_USERNAME/PASSWORD
// env vars. This grants explicit readWrite on testdb for clarity in probing.
db.createUser({
  user: "testuser",
  pwd: "testpass",
  roles: [
    { role: "readWrite", db: "testdb" }
  ]
});

// Create the 'events' collection with a simple validator
db.createCollection("events", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["event_type", "timestamp"],
      properties: {
        event_type: {
          bsonType: "string",
          description: "Type of the event"
        },
        timestamp: {
          bsonType: "date",
          description: "When the event occurred"
        },
        payload: {
          bsonType: "object",
          description: "Event payload data"
        }
      }
    }
  }
});

// Insert seed data
db.events.insertMany([
  {
    event_type: "user.signup",
    timestamp: new Date("2026-01-15T10:30:00Z"),
    payload: {
      user_id: "u-001",
      email: "alice@example.com",
      plan: "starter"
    }
  },
  {
    event_type: "order.created",
    timestamp: new Date("2026-01-15T11:00:00Z"),
    payload: {
      order_id: "ord-1001",
      user_id: "u-001",
      total: 49.99,
      currency: "USD"
    }
  },
  {
    event_type: "payment.processed",
    timestamp: new Date("2026-01-15T11:05:00Z"),
    payload: {
      payment_id: "pay-5001",
      order_id: "ord-1001",
      status: "success",
      amount: 49.99
    }
  },
  {
    event_type: "user.signup",
    timestamp: new Date("2026-01-16T09:15:00Z"),
    payload: {
      user_id: "u-002",
      email: "bob@example.com",
      plan: "professional"
    }
  },
  {
    event_type: "order.created",
    timestamp: new Date("2026-01-16T14:22:00Z"),
    payload: {
      order_id: "ord-1002",
      user_id: "u-002",
      total: 199.00,
      currency: "EUR"
    }
  }
]);

print("MongoDB initialization complete: testdb.events created with 5 seed documents.");
