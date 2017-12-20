/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <avro/platform.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/basics.h"
#include "avro/data.h"
#include "avro/io.h"
#include "avro/value.h"
#include "avro/schema.h"
#include "avro_private.h"
#include "encoding.h"


/*
 * Forward declaration; this is basically the same as avro_value_read,
 * but it doesn't reset dest first.  (Since it will have already been
 * reset in avro_value_read itself).
 */

int
read_value(avro_reader_t reader, avro_value_t *dest);


int
read_array_value(avro_reader_t reader, avro_value_t *dest);

int
read_map_value(avro_reader_t reader, avro_value_t *dest);


int
read_record_value(avro_reader_t reader, avro_value_t *dest);

int
read_union_value(avro_reader_t reader, avro_value_t *dest);


/*
 * A wrapped buffer implementation that takes control of a buffer
 * allocated using avro_malloc.
 */

struct avro_wrapped_alloc {
    const void  *original;
    size_t  allocated_size;
};

void
avro_wrapped_alloc_free(avro_wrapped_buffer_t *self);

int
avro_wrapped_alloc_new(avro_wrapped_buffer_t *dest,
                       const void *buf, size_t length);


int
read_value(avro_reader_t reader, avro_value_t *dest);

int
avro_value_read(avro_reader_t reader, avro_value_t *dest);
