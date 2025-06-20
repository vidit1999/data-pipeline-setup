import os
from typing import List, Optional, TypedDict
from enum import Enum

from fastapi import FastAPI, Depends, HTTPException, status
from sqlmodel import Field, SQLModel, Session, create_engine, select, Column, JSON

from pydantic import model_validator

# --- Database Configuration ---
# Replace with your PostgreSQL connection string
# Example: "postgresql+psycopg2://user:password@host:port/database_name"
# For Docker Compose, the host will be the service name 'db'
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

# For asynchronous operations, use asyncpg driver (e.g., "postgresql+asyncpg://...")
# and an async engine. For simplicity in a one-file example, we'll stick to
# synchronous psycopg2 driver which is widely compatible, but be aware that for
# high-concurrency async apps, an async driver is preferred.
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    """Creates database tables based on SQLModel metadata."""
    SQLModel.metadata.create_all(engine)

def get_session():
    """Dependency to get a database session."""
    with Session(engine) as session:
        yield session

# --- Models ---

class Mode(str, Enum):
    """Enum for the data loading mode."""
    UPSERT = "upsert"
    APPEND = "append"

class Transformation(TypedDict):
    col_name: str
    col_expr: str

class PipelineConfigBase(SQLModel):
    """Base model for pipeline configuration."""
    is_rds: bool = Field(description="Is the data source an RDS instance?")
    topic_name: str = Field(index=True, unique=True, description="The name of the Kafka topic or similar source.")
    transformations: List[Transformation] = Field(default_factory=list, sa_column=Column(JSON),
                                      description="List of column transformations (e.g., {col_expr: lower('user_name'), col_name: 'user_name'}).")

    mode: Mode = Field(description="Data loading mode: 'upsert' or 'append'.")
    primary_keys: List[str] = Field(default_factory=list, sa_column=Column(JSON),
                                description="Name of the primary key columns, if applicable.")
    infer_schema: bool = Field(description="Should the schema be inferred?")
    partition_col: Optional[str] = Field(default=None, description="Column to partition the data by, if applicable.")

    dq_rules: List[str] = Field(default_factory=list, sa_column=Column(JSON),
                                description="List of data quality rule names.")
    frequency: Optional[int] = Field(default=1, description="Frequency of pipeline execution in hours (e.g., 24 for daily). Default 1 hr.")
    owner_name: str = Field(description="Name of the owner of this pipeline configuration.")

    pipeline_version: int = Field(default=1, description="Version of the pipeline configuration. Incrementing this can trigger a backfill.")

    catalog_name: str = Field(description="Name of the catalog in which the table will reside.")
    table_name: str = Field(description="Name of the table in datalake.")

    @model_validator(mode='after')
    def check_primary_keys_for_upsert(self) -> 'PipelineConfigBase':
        if self.mode == Mode.UPSERT and not self.primary_keys:
            raise ValueError("primary_keys cannot be empty when mode is 'upsert'")
        return self


class PipelineConfig(PipelineConfigBase, table=True, schema="pipeline_config"):
    """Database model for pipeline configuration."""
    id: Optional[int] = Field(default=None, primary_key=True)

    @property
    def append_write_path(self) -> str:
        """Calculates the S3 path for append writes."""
        return f"s3a://my-storage-bucket/append-locations/{self.topic_name}_{self.pipeline_version}_{self.id}"

    @property
    def upsert_write_path(self) -> str:
        """Calculates the S3 path for upsert writes."""
        return f"s3a://my-storage-bucket/upsert-locations/{self.topic_name}_{self.pipeline_version}_{self.id}"

    @property
    def append_checkpoint_path(self) -> str:
        """Calculates the S3 path for append checkpoints."""
        return f"s3a://my-storage-bucket/checkpoint-append-locations/{self.topic_name}_{self.pipeline_version}_{self.id}"

    @property
    def upsert_checkpoint_path(self) -> str:
        """Calculates the S3 path for upsert checkpoints."""
        return f"s3a://my-storage-bucket/checkpoint-upsert-locations/{self.topic_name}_{self.pipeline_version}_{self.id}"


class PipelineConfigCreate(PipelineConfigBase):
    """Request model for creating a new pipeline configuration."""
    pass

class PipelineConfigRead(PipelineConfigBase):
    """Response model for reading pipeline configuration, including the ID."""
    id: int
    # These fields are calculated properties in the PipelineConfig model
    append_write_path: str
    upsert_write_path: str
    append_checkpoint_path: str
    upsert_checkpoint_path: str

class PipelineConfigUpdate(SQLModel):
    """
    Request model for partially updating an existing pipeline configuration.
    All fields are optional, as only specified fields will be updated.
    """
    is_rds: Optional[bool] = None
    # topic_name: Optional[str] = None
    transformations: Optional[List[Transformation]] = None
    mode: Optional[Mode] = None
    primary_keys: Optional[List[str]] = None
    infer_schema: Optional[bool] = None
    partition_col: Optional[str] = None
    dq_rules: Optional[List[str]] = None
    frequency: Optional[int] = None
    owner_name: Optional[str] = None
    # pipeline_version: Optional[int] = None # Can be updated directly, or via increment-version endpoint
    catalog_name: Optional[str] = None
    table_name: Optional[str] = None




# --- FastAPI Application ---

app = FastAPI(
    title="Pipeline Config Service",
    description="A service to manage and serve pipeline configurations.",
    version="1.0.0"
)

@app.on_event("startup")
def on_startup():
    """Event handler to create database tables on application startup."""
    create_db_and_tables()

@app.post("/configs/", response_model=PipelineConfigRead, status_code=status.HTTP_201_CREATED)
def create_pipeline_config(config: PipelineConfigCreate, session: Session = Depends(get_session)):
    """
    Creates a new pipeline configuration and stores it in the database.
    """
    db_config = PipelineConfig.model_validate(config)
    session.add(db_config)
    session.commit()
    session.refresh(db_config)
    return db_config

@app.get("/configs/", response_model=List[PipelineConfigRead])
def get_all_pipeline_configs(session: Session = Depends(get_session)):
    """
    Retrieves a list of all pipeline configurations.
    """
    configs = session.exec(select(PipelineConfig)).all()
    return configs

@app.get("/configs/{config_id}", response_model=PipelineConfigRead)
def get_pipeline_config(config_id: int, session: Session = Depends(get_session)):
    """
    Retrieves a pipeline configuration by its ID.
    """
    config = session.get(PipelineConfig, config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline config with ID {config_id} not found."
        )
    return config


@app.patch("/configs/{config_id}", response_model=PipelineConfigRead)
def update_pipeline_config(
    config_id: int,
    config_update: PipelineConfigUpdate,
    session: Session = Depends(get_session)
):
    """
    Partially updates an existing pipeline configuration by ID.
    """
    db_config = session.get(PipelineConfig, config_id)
    if not db_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline config with ID {config_id} not found."
        )

    # Get the updated data, excluding fields that were not provided in the request
    update_data = config_update.model_dump(exclude_unset=True)

    # Apply updates to the database model instance
    db_config.sqlmodel_update(update_data)

    # Re-validate logic for primary_keys if mode is upsert after update
    # Note: If mode is updated from APPEND to UPSERT, and primary_keys is empty
    # this will correctly catch it. If primary_keys is updated to empty, and mode
    # remains UPSERT, it will also catch it.
    if db_config.mode == Mode.UPSERT and not db_config.primary_keys:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="primary_keys cannot be empty when mode is 'upsert'"
        )

    session.add(db_config) # Add back to session to mark as modified
    session.commit()
    session.refresh(db_config) # Refresh to get latest state from DB, including computed properties
    return db_config

@app.patch("/configs/{config_id}/increment-version", response_model=PipelineConfigRead)
def increment_pipeline_version(config_id: int, session: Session = Depends(get_session)):
    """
    Increments the pipeline_version of a specific pipeline configuration,
    which can be used to trigger a backfill.
    """
    config = session.get(PipelineConfig, config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline config with ID {config_id} not found."
        )

    config.pipeline_version += 1
    session.add(config)
    session.commit()
    session.refresh(config)
    return config

# --- How to Run (Local) ---
# To run this application locally without Docker:
# 1. Save the code as a Python file (e.g., `main.py`).
# 2. Install necessary libraries:
#    `pip install fastapi uvicorn sqlmodel "psycopg2-binary<2.10"`
# 3. Ensure a PostgreSQL database is running and accessible at the DATABASE_URL.
#    You might need to create the database (`pipeline_db`) and a user if not using defaults.
#    If DATABASE_URL environment variable is not set, it defaults to `postgresql+psycopg2://user:password@localhost:5432/pipeline_db`.
# 4. Run the application using Uvicorn:
#    `uvicorn main:app --reload`
# 5. Access the API documentation at `http://127.0.0.1:8000/docs`

if __name__ == "__main__":
    import uvicorn
    # This block is for direct execution and development.
    # In a production environment, you would typically run uvicorn from the command line.
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)