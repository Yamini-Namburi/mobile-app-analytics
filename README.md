## Architecture

The project follows a layered mobile app analytics data platform architecture. Data is simulated from local CSV reports through a FastAPI mock API, ingested into Amazon S3 raw storage, transformed using AWS Glue, cataloged for Athena, and modelled into a gold-layer star schema.

```mermaid
flowchart TD

    A[Sample CSV Reports] --> B[Fast API]

    B --> C[Python Ingestion Scripts]

    C --> D[S3 Raw Layer]

    D --> E[AWS Glue Raw-to-Silver Jobs]

    E --> F[S3 Silver Layer]

    F --> G[AWS Glue Data Catalog]

    G --> H[Athena Silver Validation Queries]

    F --> I[Athena CTAS Gold Modelling]

    I --> J[S3 Gold Layer]

    J --> K[Gold Star Schema Tables]

    K --> L[Athena KPI Queries]

    M[Astro / Airflow DAGs] --> C
    M --> E

    subgraph Source Layer
        A
        B
    end

    subgraph Ingestion Layer
        C
        D
    end

    subgraph Transformation Layer
        E
        F
    end

    subgraph Catalog and Query Layer
        G
        H
    end

    subgraph Gold Analytics Layer
        I
        J
        K
        L
    end

    subgraph Orchestration Layer
        M
    end
```