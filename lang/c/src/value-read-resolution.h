int read_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest);

int read_union_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest);

int resolve_unions(avro_reader_t reader, avro_value_t *source, avro_value_t *dest);

int read_map_value_with_resolution(avro_reader_t reader, avro_value_t *source, avro_value_t *dest);

int read_array_value_with_resolution(avro_reader_t reader, avro_value_t* source, avro_value_t *dest);
