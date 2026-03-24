import io
import pandas as pd
import logging
import bson
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

def convert_json_to_parquet(json_file_path, transform_func=None, schema=None):
    """JSON -> Parquet."""
    try:
        logger.info(f"Converting JSON to Parquet: {json_file_path}")
        df = pd.read_json(json_file_path)
        if transform_func:
            df = transform_func(df)
            
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False, schema=schema)
        parquet_buffer.seek(0)
        return parquet_buffer
    except Exception as e:
        logger.error(f"Error: {e}")
        return None

def convert_bson_to_parquet(bson_file_path, batch_size=50000, transform_func=None, schema=None):
    """BSON -> Parquet with detailed progress logging."""
    try:
        logger.info(f"Opening BSON file: {bson_file_path}...")
        parquet_buffer = io.BytesIO()
        writer = None
        total_count = 0

        with open(bson_file_path, "rb") as f:
            logger.info("Starting data extraction from BSON...")
            current_batch = []
            for doc in bson.decode_file_iter(f):
                total_count += 1
                if '_id' in doc: del doc['_id']
                current_batch.append(doc)

                # Log progress every 100k records
                if total_count % 100000 == 0:
                    logger.info(f"Already read: {total_count:,} records...")

                if len(current_batch) >= batch_size:
                    logger.info(f"Writing Parquet batch at total: {total_count:,}...")
                    df_batch = pd.DataFrame(current_batch)
                    if transform_func:
                        df_batch = transform_func(df_batch)
                    
                    if schema:
                        for name in schema.names:
                            if name not in df_batch.columns:
                                if isinstance(schema.field(name).type, pa.ListType):
                                    df_batch[name] = [[] for _ in range(len(df_batch))]
                                else:
                                    df_batch[name] = None
                        df_batch = df_batch[schema.names]

                    table = pa.Table.from_pandas(df_batch, schema=schema)
                    if writer is None:
                        writer = pq.ParquetWriter(parquet_buffer, schema if schema else table.schema)
                    
                    writer.write_table(table)
                    current_batch = []

            # Final batch
            if current_batch:
                df_batch = pd.DataFrame(current_batch)
                if transform_func:
                    df_batch = transform_func(df_batch)
                if schema:
                    for name in schema.names:
                        if name not in df_batch.columns:
                            if isinstance(schema.field(name).type, pa.ListType):
                                df_batch[name] = [[] for _ in range(len(df_batch))]
                            else:
                                df_batch[name] = None
                    df_batch = df_batch[schema.names]
                
                table = pa.Table.from_pandas(df_batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_buffer, schema if schema else table.schema)
                writer.write_table(table)

        if writer: writer.close()
        parquet_buffer.seek(0)
        logger.info(f"FULLY CONVERTED! Total records: {total_count:,}")
        return parquet_buffer
    except Exception as e:
        logger.error(f"Error during BSON conversion at record {total_count}: {e}")
        return None
