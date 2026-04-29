CREATE INDEX idx_item_cooccurrence_pair ON item_cooccurrence(item_id_a, item_id_b);

CREATE INDEX idx_item_features_item_id ON item_features(item_id);

CREATE INDEX idx_user_features_user_id ON user_features(user_id);

CREATE INDEX idx_user_item_user_item ON user_item_features(user_id, item_id);

CREATE TABLE "item_cooccurrence" (
"item_id_a" TEXT,
  "item_id_b" TEXT,
  "cooccurrence_count" INTEGER
);

CREATE TABLE "item_features" (
"item_id" TEXT,
  "item_interaction_count" INTEGER,
  "unique_users" INTEGER,
  "avg_interaction_weight" REAL,
  "avg_rating_per_item" REAL,
  "title" TEXT,
  "category" TEXT,
  "price" REAL,
  "price_norm" REAL
);

CREATE TABLE "user_features" (
"user_id" INTEGER,
  "activity_frequency" INTEGER,
  "unique_items_interacted" INTEGER,
  "avg_interaction_weight" REAL,
  "avg_rating_per_user" REAL
);

CREATE TABLE "user_item_features" (
"user_id" INTEGER,
  "item_id" INTEGER,
  "interaction_count" INTEGER,
  "total_interaction_weight" REAL,
  "last_event_ts" TIMESTAMP,
  "addtocart" INTEGER,
  "transaction" INTEGER,
  "view" INTEGER
);