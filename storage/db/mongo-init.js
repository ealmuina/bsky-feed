// Check if the replica set is initialized
print("Checking if the replica set is initialized...");

try {
    const status = rs.status();
    if (status.ok === 1) {
        print("Replica set already initialized.");
    } else {
        print("Replica set not initialized, initiating...");
        rs.initiate();
        print("Replica set initialized.");
    }
} catch (e) {
    print("Error checking replica set status, attempting to initiate...");
    rs.initiate(); // Initiate replica set if it hasn't been done yet
    print("Replica set initiated.");
}

// Wait for the primary to be elected
print("Waiting for the primary node...");

while (true) {
    try {
        const isPrimary = db.isMaster().ismaster;
        if (isPrimary) {
            print("Primary node elected.");
            break;  // Exit the loop once the primary is elected
        } else {
            print("Waiting for primary node...");
        }
    } catch (e) {
        print("Error checking primary status: " + e);
    }

    // Sleep for a while to avoid overwhelming the system with requests
    sleep(1000);  // Sleep for 1 second
}

print("Starting initialization...");

const db = db.getSiblingDB("bsky_feeds");

db.createCollection('follows');
db.follows.createIndex(
    {author_id: 1, subject_id: 1},
    {unique: true}
);
db.follows.createIndex(
    {author_id: 1, uri_key: 1},
    {unique: true}
);
db.follows.createIndex({created_at: 1});
db.follows.createIndex({subject_id: 1});

db.createCollection('interactions');
db.interactions.createIndex(
    {author_id: 1, post_id: 1, kind: 1},
    {unique: true}
);
db.interactions.createIndex(
    {author_id: 1, uri_key: 1},
    {unique: true}
);
db.interactions.createIndex({created_at: 1});
db.interactions.createIndex({post_id: 1});

db.createCollection('posts');
db.posts.createIndex(
    {author_id: 1, uri_key: 1},
    {unique: true}
);
db.posts.createIndex({created_at: 1});

db.createCollection('users');
db.users.createIndex(
    {did: 1},
    {unique: true}
);
db.users.createIndex({created_at: 1});
db.users.createIndex({last_update: 1});

print("DB initialized!")