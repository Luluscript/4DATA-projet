// Créer les collections de base pour l'application
db = db.getSiblingDB('supmap');

// db.createCollection('users');
// db.users.createIndex({ email: 1 }, { unique: true });
// db.users.createIndex({ username: 1 }, { unique: true });

// db.createCollection('activities');
// db.activities.createIndex({ userId: 1 });
// db.activities.createIndex({ type: 1 });
// db.activities.createIndex({ date: 1 });

// db.createCollection('social_interactions');
// db.social_interactions.createIndex({ userId: 1 });
// db.social_interactions.createIndex({ activityId: 1 });
// db.social_interactions.createIndex({ type: 1 });

// db.createCollection('sensor_data');
// db.sensor_data.createIndex({ userId: 1 });
// db.sensor_data.createIndex({ timestamp: 1 });
// db.sensor_data.createIndex({ deviceId: 1 });

print("MongoDB initialized for supmap platform ");