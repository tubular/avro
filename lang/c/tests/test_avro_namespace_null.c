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

#include "avro.h"
#include "schema.h"
#include <stdio.h>
#include <stdlib.h>

static void test_namespace_null()
{
	int rc;
	avro_schema_t base;
	avro_schema_error_t serror;

	static char *JSON =
		"{"
		"  \"type\": \"record\","
		"  \"namespace\": \"\","
		"  \"name\": \"X\","
		"  \"fields\": ["
		"    {\"name\": \"x\", \"type\": \"int\"}"
		"  ]"
		"}";

	rc = avro_schema_from_json(JSON, strlen(JSON), &base, &serror);
	if (rc != 0) {
		fprintf(stderr,
			"Error parsing Avro schema:\n%s\n%s\n",
			JSON, avro_strerror());
		exit(EXIT_FAILURE);
	}

	struct avro_record_schema_t *record = avro_schema_to_record(base);
	if (record->space != NULL) {
		fprintf(stderr, "Expected namespace = NULL\n");
		exit(EXIT_FAILURE);
	}
}

int main(int argc, char *argv[])
{
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	test_namespace_null();
	return EXIT_SUCCESS;
}
