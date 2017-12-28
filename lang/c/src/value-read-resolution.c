#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/basics.h"
#include "avro/data.h"
#include "avro/io.h"
#include "avro/value.h"
#include "avro_private.h"
#include "encoding.h"
#include "value-read.h"
#include "st.h"


/*
 * Populate avro_value_t with default value specified in schema.
 */
static int read_default_value(avro_schema_t reader_schema, const char* field_name, avro_value_t *dest)
{
    json_t* def_val = avro_schema_record_field_default(reader_schema, field_name);
    switch (avro_value_get_type(dest)) {
        case AVRO_STRING:
        case AVRO_BYTES:
        {
            char* val = avro_get_default_string_value(def_val);
            size_t size = (strlen(val) + 1) * sizeof(char);
            avro_wrapped_buffer_t  buf;
            return avro_value_set_string_len(dest, val, size);
        }
        case AVRO_INT32:
        {
            int32_t val = avro_get_default_int_value(def_val);
            return avro_value_set_int(dest, val);
        }
        case AVRO_INT64:
        {
            int64_t val = avro_get_default_int_value(def_val);
            return avro_value_set_long(dest, val);
        }
        case AVRO_FLOAT:
        {
            float val = avro_get_default_float_value(def_val);
            return avro_value_set_float(dest, val);
        }
        case AVRO_DOUBLE:
        {
            double val = avro_get_default_double_value(def_val);
            return avro_value_set_double(dest, val);
        }
        case AVRO_BOOLEAN:
        {
            bool val = avro_get_default_bool_value(def_val);
            return avro_value_set_boolean(dest, val);
        }
    }
    avro_set_error("Cannot read default value for specified type.");
    return EINVAL;
}

/*
 * Read record with schema resolution.
 */
static int read_record_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    /*
     * To perform the schema resolution we keep two values here: `source` and `dest`.
     * `source` value is the actual value in writers schema,
     * `dest` value is the value of readers schema that we have to resolve `source` into.
     * First, we read all the writers fields. If the field is in `source` schema, but not in `dest`
     * we skip it. If the field is in both reader and writer, then we trying to perform schema resolution 
     * for these fields. If the field is in reader's schema, but not writer's schema it must have default
     * value set. If the default is set, we read this value into `dest` value.
     */
    int  rval;
    size_t  field_count;
    size_t  i;
    
    avro_schema_t  record_schema_reader = avro_value_get_schema(dest);
    avro_schema_t  record_schema_writer = avro_value_get_schema(source);
    
    size_t reader_fields_count;
    size_t writers_fields_count;
    
    check(rval, avro_value_get_size(dest, &reader_fields_count));
    check(rval, avro_value_get_size(source, &writers_fields_count));
    
    // map to keep track of already resolved fields.
    st_table* reader_fields_table = st_init_strtable();
    
    // loop writers fields
    for(i = 0; i<writers_fields_count; ++i)
    {
        avro_value_t field;
        check(rval, avro_value_get_by_index(source, i, &field, NULL));
        char* field_name =  avro_schema_record_field_name(record_schema_writer, i);
        
        int reader_index = avro_schema_record_field_get_index(record_schema_reader, field_name);
        
        if (reader_index > -1) {
            // ok, reader's schema has the field with the same name too,
            // lets do schema resolution.
            avro_value_t reader_field;
            check(rval, avro_value_get_by_index(dest, reader_index, &reader_field, NULL));
            
            check(rval, read_value_with_resolution(reader, &field, &reader_field));

            st_insert(reader_fields_table, (st_data_t) field_name, &reader_field);
            
        } else {
            // reader's schema doesn't have this field, skip it.
            avro_schema_t  field_schema = avro_schema_record_field_get_by_index(record_schema_writer, i);
            check(rval, avro_skip_data(reader, field_schema));
        }
    }
    
    // Check remaining reader's schema fields for default values.
    for(i = 0; i<reader_fields_count; ++i)
    {
        avro_value_t field;
        check(rval, avro_value_get_by_index(dest, i, &field, NULL));
        char* readers_field_name = avro_schema_record_field_name(record_schema_reader, i);
        
        st_data_t  data;
        if(st_lookup(reader_fields_table, (st_data_t) readers_field_name, &data) == 0)
        {
            // this field is not in writer's schema, it must have default value.
            json_t* def_val = avro_schema_record_field_default(record_schema_reader, readers_field_name);
            int has_def = def_val == NULL ? 0 : 1;
            if(has_def == 0)
            {
                avro_set_error("Schema resolution error! There must be default "
                               "value set for fields missing in writer's schema.");
                return EINVAL;
            }
            // read default value
            read_default_value(record_schema_reader, readers_field_name, &field);
        }
    }

    if(reader_fields_table)
    {
        // cleanup
        st_free_table(reader_fields_table);
    }
    return 0;
}

// Simple macro to check if dest field is union before setting the value.
#define AVRO_RESOLVE_SOURCE(reader, source, dest, type) \
    (avro_value_get_type(dest) == AVRO_UNION ? read_union_value_with_resolution(reader, source, dest) : \
        resolve_##type##_source(reader, source, dest))

static int resolve_type_boolean_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    int8_t  val;
    check_prefix(rval, avro_binary_encoding.
                 read_boolean(reader, &val),
                 "Cannot read boolean value: ");
    return avro_value_set_boolean(dest, val);
}

static int resolve_type_bytes_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    char  *bytes;
    int64_t  len;
    check_prefix(rval, avro_binary_encoding.
                 read_bytes(reader, &bytes, &len),
                 "Cannot read bytes value: ");
    
    /*
     * read_bytes allocates an extra byte to always
     * ensure that the data is NUL terminated, but
     * that byte isn't included in the length.  We
     * include that extra byte in the allocated
     * size, but not in the length of the buffer.
     */
    
    avro_wrapped_buffer_t  buf;
    check(rval, avro_wrapped_alloc_new(&buf, bytes, len+1));
    switch(avro_value_get_type(dest))
    {
            // Bytes type is promotable to String
        case AVRO_BYTES:
            return avro_value_give_bytes(dest, &buf);
        case AVRO_STRING:
            return avro_value_give_string_len(dest, &buf);
    }
    avro_set_error("Invalid type in readers schema.");
    return EINVAL;
}

static int resolve_type_double_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    double  val;
    check_prefix(rval, avro_binary_encoding.
                 read_double(reader, &val),
                 "Cannot read double value: ");
    return avro_value_set_double(dest, val);
}

static int resolve_type_float_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    float  val;
    check_prefix(rval, avro_binary_encoding.
                 read_float(reader, &val),
                 "Cannot read float value: ");

    // Float is promotable to Double
    switch(avro_value_get_type(dest)){
        case AVRO_FLOAT:
            return avro_value_set_float(dest, val);
        case AVRO_DOUBLE:
            return avro_value_set_double(dest, (double)val);
    }
    avro_set_error("Invalid type in readers schema.");
    return EINVAL;
}

static int resolve_type_long_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    int64_t  val;
    check_prefix(rval, avro_binary_encoding.
                 read_long(reader, &val),
                 "Cannot read long value: ");
    
    // Int64 is promotable to Float and Double
    switch(avro_value_get_type(dest)){
        case AVRO_FLOAT:
            return avro_value_set_float(dest, (float)val);
        case AVRO_DOUBLE:
            return avro_value_set_double(dest, (double)val);
        case AVRO_INT64:
            return avro_value_set_long(dest, val);
    }
    avro_set_error("Invalid type in readers schema.");
    return EINVAL;
}

static int resolve_type_int_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    int32_t  val;
    check_prefix(rval, avro_binary_encoding.
                 read_int(reader, &val),
                 "Cannot read int value: ");
    // Int32 is promotable to Int64, Float and Double
    switch(avro_value_get_type(dest)){
        case AVRO_INT32:
            return avro_value_set_int(dest, val);
        case AVRO_INT64:
            return avro_value_set_long(dest, (int64_t)val);
        case AVRO_FLOAT:
            return avro_value_set_float(dest, (float)val);
        case AVRO_DOUBLE:
            return avro_value_set_double(dest, (double)val);
    }
    avro_set_error("Invalid type in readers schema.");
    return EINVAL;
}

static int resolve_type_null_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    check_prefix(rval, avro_binary_encoding.
                 read_null(reader),
                 "Cannot read null value: ");
    return avro_value_set_null(dest);
}

static int resolve_type_string_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    char  *str;
    int64_t  size;
    
    /*
     * read_string returns a size that includes the
     * NUL terminator, and the free function will be
     * called with a size that also includes the NUL
     */
    check_prefix(rval, avro_binary_encoding.
                 read_string(reader, &str, &size),
                 "Cannot read string value: ");
    
    avro_wrapped_buffer_t  buf;
    check(rval, avro_wrapped_alloc_new(&buf, str, size));
    return avro_value_give_string_len(dest, &buf);
}

static int resolve_type_enum_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    int64_t  val;
    check_prefix(rval, avro_binary_encoding.
                 read_long(reader, &val),
                 "Cannot read enum value: ");
    return avro_value_set_enum(dest, val);
}

static int resolve_type_fixed_source(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    avro_schema_t  schema = avro_value_get_schema(dest);
    char *bytes;
    int64_t size = avro_schema_fixed_size(schema);
    
    bytes = (char *) avro_malloc(size);
    if (!bytes) {
        avro_prefix_error("Cannot allocate new fixed value");
        return ENOMEM;
    }
    rval = avro_read(reader, bytes, size);
    if (rval) {
        avro_prefix_error("Cannot read fixed value: ");
        avro_free(bytes, size);
        return rval;
    }
    
    avro_wrapped_buffer_t  buf;
    rval = avro_wrapped_alloc_new(&buf, bytes, size);
    if (rval != 0) {
        avro_free(bytes, size);
        return rval;
    }
    
    return avro_value_give_fixed(dest, &buf);
}

/*
 * Resolve value of writers schema into value of readers schema.
 */
int read_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int  rval;
    switch (avro_value_get_type(source))
    {
        case AVRO_BOOLEAN:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_boolean);
        }
        case AVRO_BYTES:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_bytes);
        }
        case AVRO_DOUBLE:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_double);
        }
        case AVRO_FLOAT:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_float);
        }
        case AVRO_INT64:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_long);
        }
        case AVRO_INT32:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_int);
        }
        case AVRO_NULL:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_null);
        }
        case AVRO_STRING:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_string);
        }
        case AVRO_ARRAY:
        {
            return read_array_value_with_resolution(reader, source, dest);
        }
        case AVRO_ENUM:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_enum);
        }
        case AVRO_FIXED:
        {
            return AVRO_RESOLVE_SOURCE(reader, source, dest, type_fixed);
        }
        case AVRO_MAP:
        {
            return read_map_value_with_resolution(reader, source, dest);
        }
        case AVRO_RECORD:
        {
            return read_record_value_with_resolution(reader, source, dest);
        }
        case AVRO_UNION:
        {
            return read_union_value_with_resolution(reader, source, dest);
        }
        default:
        {
            avro_set_error("Unknown schema type");
            return EINVAL;
        }

    }
    
    return 0;
}

int avro_value_read_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int  rval;
    check(rval, avro_value_reset(dest));
    avro_schema_t wschema = avro_value_get_schema(source);
    avro_schema_t rschema = avro_value_get_schema(dest);
    if(avro_schema_match(wschema, rschema) == 0)
    {
        avro_set_error("Schema resolution error!");
        return EINVAL;
    }
    return read_value_with_resolution(reader, source, dest);
}

int
read_union_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    avro_schema_t wschema = avro_value_get_schema(source);
    avro_schema_t rschema = avro_value_get_schema(dest);
    
    int wtype = avro_value_get_type(source);
    int rtype = avro_value_get_type(dest);
    if ( (rtype == AVRO_UNION) && (wtype == AVRO_UNION))
    {
        return resolve_unions(reader, source, dest);
    }
    else if ((rtype == AVRO_UNION) && (wtype != AVRO_UNION))
    {
        // reader is union, writer is not
        int64_t  branch_count_dest;
        branch_count_dest = avro_schema_union_size(rschema);
        int i;
        for(i = 0; i<branch_count_dest; ++i)
        {
            avro_schema_t  branch_schema = avro_schema_union_branch(rschema, i);
            if(avro_schema_match(wschema, branch_schema))
            {
                avro_value_t  branch_dest;
                avro_value_set_branch(dest, i, &branch_dest);
                return read_value_with_resolution(reader, source, &branch_dest);
            }
        }
        avro_set_error("There is no compatible branch in readers schema!");
        return EINVAL;
    }
    else if ((rtype != AVRO_UNION) && (wtype == AVRO_UNION))
    {
        // writer is union, reader is not
        // must read discriminant value (long) from buffer
        // before reading actual branch value.

        int rval;
        int64_t discriminant;
        int64_t  branch_count;
        avro_value_t  branch;

        check_prefix(rval, avro_binary_encoding.
                     read_long(reader, &discriminant),
                     "Cannot read union discriminant: ");

        branch_count = avro_schema_union_size(wschema);

        if (discriminant < 0 || discriminant >= branch_count) {
            avro_set_error("Invalid union discriminant value: (%d)",
                           discriminant);
            return EINVAL;
        }

        check(rval, avro_value_set_branch(source, discriminant, &branch));
        check(rval, avro_value_get_current_branch(source, &branch));
        return read_value_with_resolution(reader, &branch, dest);
    }
    else
    {
        // Shouldn't be here
        avro_set_error("Cannot resolve unions.");
        return EINVAL;
    }
}

/*
 * Read of maps, arrays and unions is mostly the same as when resolution
 * is not needed, the only thing is that we need to keep track of `source`
 * value to be able to resolve schemas recursively.
 */
int resolve_unions(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int rval;
    int64_t discriminant;
    avro_schema_t  union_schema_source;
    int64_t  branch_count_source;
    avro_value_t  branch_source;
    
    avro_schema_t  union_schema_dest;
    int64_t  branch_count_dest;
    avro_value_t  branch_dest;
    
    check_prefix(rval, avro_binary_encoding.
                 read_long(reader, &discriminant),
                 "Cannot read union discriminant: ");

    union_schema_source = avro_value_get_schema(source);
    branch_count_source = avro_schema_union_size(union_schema_source);
    
    union_schema_dest = avro_value_get_schema(dest);
    branch_count_dest = avro_schema_union_size(union_schema_dest);
    if (discriminant < 0 || discriminant >= branch_count_source) {
        avro_set_error("Invalid union discriminant value: (%d)",
                       discriminant);
        return EINVAL;
    }
    
    check_prefix(rval,
                 avro_value_set_branch(source, discriminant, &branch_source),
                 "Cannot set current branch");
    avro_schema_t source_branch_schema = avro_value_get_schema(&branch_source);
    int i;
    for(i = 0; i<branch_count_dest; ++i)
    {
        // The first schema in the reader's union that matches the selected writer's union schema
        // is recursively resolved against it. if none match, an error is signalled.
        avro_schema_t branch = avro_schema_union_branch(union_schema_dest, i);
        if(avro_schema_match(source_branch_schema, branch))
        {
            avro_value_t  branch_dest;
            avro_value_set_branch(dest, i, &branch_dest);
            return read_value_with_resolution(reader, &branch_source, &branch_dest);
        }
    }
    return 0;

}

int
read_array_value_with_resolution(avro_reader_t reader, avro_value_t* source, avro_value_t *dest)
{
    int  rval;
    size_t  i;          /* index within the current block */
    size_t  index = 0;  /* index within the entire array */
    int64_t  block_count;
    int64_t  block_size;
    
    check_prefix(rval, avro_binary_encoding.
                 read_long(reader, &block_count),
                 "Cannot read array block count: ");
    
    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            check_prefix(rval, avro_binary_encoding.
                         read_long(reader, &block_size),
                         "Cannot read array block size: ");
        }
        
        for (i = 0; i < (size_t) block_count; i++, index++) {
            avro_value_t  child;
            check(rval, avro_value_append(dest, &child, NULL));
            
            // need source's child to read data into.
            avro_value_t source_child;
            check(rval, avro_value_append(source, &source_child, NULL));
            
            check(rval, read_value_with_resolution(reader, &source_child, &child));
        }
        
        check_prefix(rval, avro_binary_encoding.
                     read_long(reader, &block_count),
                     "Cannot read array block count: ");
    }
    
    return 0;
}

int
read_map_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest)
{
    int  rval;
    size_t  i;          /* index within the current block */
    size_t  index = 0;  /* index within the entire array */
    int64_t  block_count;
    int64_t  block_size;
    
    check_prefix(rval, avro_binary_encoding.read_long(reader, &block_count),
                 "Cannot read map block count: ");
    
    while (block_count != 0) {
        if (block_count < 0) {
            block_count = block_count * -1;
            check_prefix(rval, avro_binary_encoding.
                         read_long(reader, &block_size),
                         "Cannot read map block size: ");
        }
        
        for (i = 0; i < (size_t) block_count; i++, index++) {
            char *key;
            int64_t key_size;
            avro_value_t  child;
            check_prefix(rval, avro_binary_encoding.
                         read_string(reader, &key, &key_size),
                         "Cannot read map key: ");
            
            rval = avro_value_add(dest, key, &child, NULL, NULL);
            if (rval) {
                avro_free(key, key_size);
                return rval;
            }
            
            // need source's child to read data into.
            avro_value_t  child_source;
            rval = avro_value_add(source, key, &child_source, NULL, NULL);
            if (rval) {
                avro_free(key, key_size);
                return rval;
            }
            
            rval = read_value_with_resolution(reader, &child_source, &child);
            if (rval) {
                avro_free(key, key_size);
                return rval;
            }
            
            avro_free(key, key_size);
        }
        
        check_prefix(rval, avro_binary_encoding.
                     read_long(reader, &block_count),
                     "Cannot read map block count: ");
    }
    
    return 0;
}
