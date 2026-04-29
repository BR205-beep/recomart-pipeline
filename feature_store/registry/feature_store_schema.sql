CREATE TABLE "feature_metadata" (
"feature_view" TEXT,
  "entity_keys" TEXT,
  "feature_name" TEXT,
  "dtype" TEXT,
  "source_path" TEXT,
  "source_table" TEXT,
  "transformation_logic" TEXT,
  "version" TEXT,
  "retrieval_modes" TEXT,
  "created_ts" TEXT
);

CREATE TABLE "feature_store_versions" (
"version" TEXT,
  "created_ts" TEXT,
  "store_type" TEXT,
  "offline_store" TEXT,
  "config_path" TEXT,
  "metadata_path" TEXT
);

CREATE TABLE "item_cooccurrence_v1" (
"item_id_a" INTEGER,
  "item_id_b" INTEGER,
  "cooccurrence_count" INTEGER
);

CREATE TABLE "item_features_v1" (
"item_id" INTEGER,
  "item_interaction_count" INTEGER,
  "unique_users" INTEGER,
  "avg_interaction_weight" REAL,
  "avg_rating_per_item" REAL,
  "title" TEXT,
  "category" TEXT,
  "price" REAL,
  "price_norm" REAL
);

CREATE TABLE "user_features_v1" (
"user_id" INTEGER,
  "activity_frequency" INTEGER,
  "unique_items_interacted" INTEGER,
  "avg_interaction_weight" REAL,
  "avg_rating_per_user" REAL
);

CREATE TABLE "user_item_features_v1" (
"user_id" INTEGER,
  "item_id" INTEGER,
  "interaction_count" INTEGER,
  "total_interaction_weight" REAL,
  "last_event_ts" REAL,
  "addtocart" INTEGER,
  "transaction" INTEGER,
  "view" INTEGER
);