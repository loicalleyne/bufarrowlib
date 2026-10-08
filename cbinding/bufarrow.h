/*
 * bufarrow.h — C API for bufarrowlib (protobuf → Arrow/Parquet transcoder).
 *
 * Every function returns an int status: 0 = success, -1 = error.
 * On error, call BufarrowLastError(handle) to retrieve the message.
 * Strings returned by BufarrowFieldNames / BufarrowLastError are
 * malloc'd; free them with BufarrowFreeString().
 *
 * Arrow data is exchanged via the Arrow C Data Interface structs
 * (ArrowSchema / ArrowArray) defined below per the canonical ABI.
 */

#ifndef BUFARROW_H
#define BUFARROW_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Arrow C Data Interface ABI structs ─────────────────────────────── */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t     flags;
    int64_t     n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t  length;
    int64_t  null_count;
    int64_t  offset;
    int64_t  n_buffers;
    int64_t  n_children;
    const void   **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

/* ── Opaque handle type ─────────────────────────────────────────────── */

typedef void *BufarrowHandle;
typedef void *BufarrowHyperTypeHandle;

typedef struct {
    uint8_t kind;
    int64_t i64;
    double f64;
    char *ptr;
    int32_t len;
} BufarrowMetaValue;

/* ── Lifecycle ──────────────────────────────────────────────────────── */

/**
 * Create a Transcoder from a .proto file.
 *
 * @param proto_path   Path to the .proto file.
 * @param msg_name     Top-level message name to transcode.
 * @param import_paths NULL-terminated array of import directories (may be NULL).
 * @param n_paths      Number of import paths.
 * @param opts_json    JSON-encoded option overrides (may be NULL). Keys:
 *                     custom_proto, custom_message, custom_import_paths,
 *                     denorm_columns, denorm_metadata_columns,
 *                     json_termination (array of fully qualified names),
 *                     json_termination_auto, well_known_types and
 *                     prune_empty_messages (booleans). Unknown keys are an
 *                     error.
 * @param out_handle   Receives the new handle on success.
 * @return 0 on success, -1 on error (call BufarrowGetGlobalError, then
 *         BufarrowGetGlobalErrorInfo for a cyclic type).
 */
extern int BufarrowNewFromFile(
    const char *proto_path,
    const char *msg_name,
    const char **import_paths,
    int         n_paths,
    const char *opts_json,
    BufarrowHandle *out_handle
);

/**
 * Create a Transcoder from a YAML config file.
 *
 * @param config_path  Path to the YAML config.
 * @param out_handle   Receives the new handle on success.
 * @return 0 on success, -1 on error.
 */
extern int BufarrowNewFromConfig(
    const char *config_path,
    BufarrowHandle *out_handle
);

/**
 * Create a Transcoder from a YAML config string.
 *
 * @param config_yaml  YAML config content as a string.
 * @param config_len   Length of config_yaml.
 * @param out_handle   Receives the new handle on success.
 * @return 0 on success, -1 on error.
 */
extern int BufarrowNewFromConfigString(
    const char     *config_yaml,
    int             config_len,
    BufarrowHandle *out_handle
);

/** Clone a Transcoder for parallel use. */
extern int BufarrowClone(BufarrowHandle handle, BufarrowHandle *out_handle);

/** Free a Transcoder handle and its resources. */
extern void BufarrowFree(BufarrowHandle handle);

/* ── Ingestion ──────────────────────────────────────────────────────── */

/** Append raw protobuf bytes (requires HyperType). */
extern int BufarrowAppendRaw(
    BufarrowHandle handle,
    const void *data,
    int         data_len
);

/** Append raw protobuf bytes to denormalizer (requires HyperType + denorm plan). */
extern int BufarrowAppendDenormRaw(
    BufarrowHandle handle,
    const void *data,
    int         data_len,
    const BufarrowMetaValue *meta_values,
    int         n_meta,
    int        *out_rows
);

/** Append merged base+custom raw bytes. */
extern int BufarrowAppendRawMerged(
    BufarrowHandle handle,
    const void *base_data,
    int         base_len,
    const void *custom_data,
    int         custom_len
);

/** Append merged base+custom raw bytes to denormalizer. */
extern int BufarrowAppendDenormRawMerged(
    BufarrowHandle handle,
    const void *base_data,
    int         base_len,
    const void *custom_data,
    int         custom_len,
    const BufarrowMetaValue *meta_values,
    int         n_meta,
    int        *out_rows
);

/* ── Flush (Arrow C Data Interface) ─────────────────────────────────── */

/**
 * Flush the record builder into an Arrow RecordBatch.
 * Caller imports via the C Data Interface pointers.
 */
extern int BufarrowFlush(
    BufarrowHandle      handle,
    void               *out_array,   /* struct ArrowArray* */
    void               *out_schema   /* struct ArrowSchema* */
);

/** Flush the denormalizer into an Arrow RecordBatch. */
extern int BufarrowFlushDenorm(
    BufarrowHandle      handle,
    void               *out_array,   /* struct ArrowArray* */
    void               *out_schema   /* struct ArrowSchema* */
);

/* ── Schema ─────────────────────────────────────────────────────────── */

/** Export the Arrow schema for the full message. */
extern int BufarrowGetSchema(
    BufarrowHandle      handle,
    void               *out_schema   /* struct ArrowSchema* */
);

/** Export the Arrow schema for the denormalized view. */
extern int BufarrowGetDenormSchema(
    BufarrowHandle      handle,
    void               *out_schema   /* struct ArrowSchema* */
);

/* ── Parquet ────────────────────────────────────────────────────────── */

/** Write buffered records to a Parquet file. */
extern int BufarrowWriteParquet(
    BufarrowHandle handle,
    const char    *file_path
);

/** Write buffered denorm records to a Parquet file. */
extern int BufarrowWriteParquetDenorm(
    BufarrowHandle handle,
    const char    *file_path
);

/**
 * Read selected columns from a Parquet file into an Arrow RecordBatch.
 *
 * @param handle       Transcoder handle.
 * @param file_path    Path to the Parquet file.
 * @param columns_json JSON array of column indices (e.g. "[0,1,3]"), or NULL for all.
 * @param out_array    Receives the ArrowArray.
 * @param out_schema   Receives the ArrowSchema.
 * @return 0 on success, -1 on error.
 */
extern int BufarrowReadParquet(
    BufarrowHandle  handle,
    const char     *file_path,
    const char     *columns_json,
    void           *out_array,
    void           *out_schema
);

/* ── Info ───────────────────────────────────────────────────────────── */

/** Return top-level field names as a JSON array string (caller must free). */
extern char *BufarrowFieldNames(BufarrowHandle handle);

/** Return last error message for handle (caller must free). */
extern char *BufarrowLastError(BufarrowHandle handle);

/** Free a string returned by BufarrowFieldNames or BufarrowLastError. */
extern void BufarrowFreeString(char *s);

/** Return library version string (caller must free). */
extern char *BufarrowVersion(void);

/**
 * Return and clear the message of the last error raised before a handle
 * existed, for example by a failed BufarrowNewFromFile (caller must free).
 */
extern char *BufarrowGetGlobalError(void);

/**
 * Return and clear a JSON object describing the last global error, or ""
 * when it has no structured form (caller must free). Call it after
 * BufarrowGetGlobalError. A cyclic message type is reported as
 *   {"kind":"cyclic_type","type":"pkg.Type","path":"field.path"}
 * where "type" is the name to pass in opts_json "json_termination".
 */
extern char *BufarrowGetGlobalErrorInfo(void);

/* ── Schema inspection ──────────────────────────────────────────────── */

/**
 * List every cyclic message type reachable from msg_name.
 *
 * Writes a JSON array of fully qualified names, sorted, to *out_json
 * ("[]" when there are none; free with BufarrowFreeString). Pass the array as
 * opts_json "json_termination" to build a Transcoder or Pool for the schema.
 *
 * @return 0 on success, -1 on error (call BufarrowGetGlobalError).
 */
extern int BufarrowCyclicTypes(
    const char  *proto_path,
    const char  *msg_name,
    const char **import_paths,
    int          n_paths,
    char       **out_json
);

/* ── HyperType ──────────────────────────────────────────────────────── */

/**
 * Create a HyperType coordinator for PGO-enabled ingestion.
 *
 * @param proto_path   Path to the .proto file.
 * @param msg_name     Message name.
 * @param import_paths NULL-terminated array of import directories (may be NULL).
 * @param n_paths      Number of import paths.
 * @param threshold    Auto-recompile threshold (0 = manual).
 * @param rate         Sampling rate for profiling (0, 1].
 * @param out_handle   Receives the new HyperType handle.
 * @return 0 on success, -1 on error.
 */
extern int BufarrowNewHyperType(
    const char *proto_path,
    const char *msg_name,
    const char **import_paths,
    int         n_paths,
    int64_t     threshold,
    double      rate,
    BufarrowHyperTypeHandle *out_handle
);

/** Free a HyperType handle. */
extern void BufarrowFreeHyperType(BufarrowHyperTypeHandle handle);

/**
 * Create a Transcoder from a .proto file with a shared HyperType.
 * Same as BufarrowNewFromFile but attaches the HyperType handle.
 */
extern int BufarrowNewFromFileWithHyperType(
    const char *proto_path,
    const char *msg_name,
    const char **import_paths,
    int         n_paths,
    const char *opts_json,
    BufarrowHyperTypeHandle hyper_handle,
    BufarrowHandle *out_handle
);

/* ── Pool — Go-managed concurrent ingestion ─────────────────────────── */

typedef void *BufarrowPoolHandle;

/**
 * Create a Pool that fans out message ingestion across N Go goroutines.
 *
 * Workers are clones of a base Transcoder built from proto_path/msg_name.
 * All concurrency is managed inside Go; Python stays single-threaded.
 *
 * @param proto_path   Path to the .proto file.
 * @param msg_name     Top-level message name.
 * @param import_paths NULL-terminated array of import directories (may be NULL).
 * @param n_paths      Number of import paths.
 * @param opts_json    JSON-encoded option overrides (may be NULL).
 *                     Include "denorm_columns" to use the denorm path.
 *                     Keys are the same as for BufarrowNewFromFile.
 * @param workers      Number of worker goroutines (0 → GOMAXPROCS).
 * @param capacity     Job channel capacity (0 → workers×64).
 * @param out_handle   Receives the new Pool handle on success.
 * @return 0 on success, -1 on error (call BufarrowGetGlobalError).
 */
extern int BufarrowPoolNew(
    const char *proto_path,
    const char *msg_name,
    const char **import_paths,
    int         n_paths,
    const char *opts_json,
    int         workers,
    int         capacity,
    BufarrowPoolHandle *out_handle
);

/**
 * Create a Pool with a shared HyperType for PGO-enabled ingestion.
 * Same as BufarrowPoolNew but attaches a pre-warmed HyperType handle.
 */
extern int BufarrowPoolNewWithHyperType(
    const char *proto_path,
    const char *msg_name,
    const char **import_paths,
    int         n_paths,
    const char *opts_json,
    BufarrowHyperTypeHandle hyper_handle,
    int         workers,
    int         capacity,
    BufarrowPoolHandle *out_handle
);

/**
 * Submit one serialized proto message to the pool.
 *
 * Copies [data, data+data_len) before returning; caller may free/reuse
 * the buffer immediately.  Blocks only when the job channel is full,
 * providing automatic backpressure to the caller's consumer loop.
 *
 * @return 0 on success, -1 on error (call BufarrowPoolLastError).
 */
extern int BufarrowPoolSubmit(
    BufarrowPoolHandle handle,
    const void *data,
    int         data_len,
    const BufarrowMetaValue *meta_values,
    int         n_meta
);

/**
 * Submit one base+custom byte pair (merged ingestion path).
 * Both buffers are copied before return.
 */
extern int BufarrowPoolSubmitMerged(
    BufarrowPoolHandle handle,
    const void *base_data,
    int         base_len,
    const void *custom_data,
    int         custom_len,
    const BufarrowMetaValue *meta_values,
    int         n_meta
);

/**
 * Drain all workers, merge per-worker Arrow batches, export via C Data Interface.
 *
 * After this call the pool is immediately ready for the next submit/flush window.
 * out_array and out_schema must point to caller-allocated ArrowArray / ArrowSchema
 * structs (same convention as BufarrowFlush).
 *
 * @return 0 on success, -1 on error (call BufarrowPoolLastError).
 */
extern int BufarrowPoolFlush(
    BufarrowPoolHandle handle,
    void              *out_array,   /* struct ArrowArray*  */
    void              *out_schema   /* struct ArrowSchema* */
);

/** Approximate number of messages queued but not yet flushed. */
extern int BufarrowPoolPending(BufarrowPoolHandle handle);

/** Retrieve and clear the last pool error string (caller must free with BufarrowFreeString). */
extern char *BufarrowPoolLastError(BufarrowPoolHandle handle);

/** Free a Pool handle and all its worker resources. */
extern void BufarrowPoolFree(BufarrowPoolHandle handle);

#ifdef __cplusplus
}
#endif

#endif /* BUFARROW_H */
