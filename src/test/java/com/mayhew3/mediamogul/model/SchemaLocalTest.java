package com.mayhew3.mediamogul.model;

import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.postgresobject.dataobject.DataSchema;
import com.mayhew3.postgresobject.db.DatabaseEnvironment;
import com.mayhew3.postgresobject.model.PostgresSchemaTest;

public class SchemaLocalTest extends PostgresSchemaTest {

  @Override
  public DataSchema getDataSchema() {
    return MediaMogulSchema.schema;
  }

  @Override
  public DatabaseEnvironment getDatabaseEnvironment() {
    return DatabaseEnvironments.environments.get("local");
  }
}