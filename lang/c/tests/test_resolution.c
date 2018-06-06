#include "avro.h"
#include "avro_private.h"

#define CHECK_CASE(expected, actual, msg) \
    if((expected) != (actual)) \
    { \
        fprintf(stderr, msg); \
        return EXIT_FAILURE; \
    }

#define CHECK_STRING_CASE(expected, actual, msg) \
    if(strcmp((expected), (actual)) != 0) \
    { \
        fprintf(stderr, msg); \
        return EXIT_FAILURE; \
    }

int test_resolution()
{
    const char* in_schema_json =
    "{"
    "  \"type\": \"record\","
    "  \"name\": \"test\","
    "  \"fields\": ["
    "    { \"name\": \"a\", \"type\": \"int\" },"
    "    { \"name\": \"b\", \"type\": \"float\" },"
    "    { \"name\": \"c\", \"type\": \"double\" },"
    "    { \"name\": \"d\", \"type\": { \"type\": \"array\", \"items\": \"int\" }  },"
    "    { \"name\": \"e\", \"type\": [\"null\", \"float\"] },"
    "    { \"name\": \"f\", \"type\": "
    "       { \"type\": \"record\", \"name\": \"sub\", \"fields\": ["
    "           { \"name\": \"sub_a\", \"type\": \"int\" }"
    "       ]}"
    "    },"
    "   {\"name\": \"j\", \"type\": \"bytes\"}"
    "  ]"
    "}";

    const char* out_schema_json =
    "{"
    "  \"type\": \"record\","
    "  \"name\": \"test\","
    "  \"fields\": ["
    "    { \"name\": \"a\", \"type\": \"float\" },"
    "    { \"name\": \"b\", \"type\": \"double\" },"
    "    { \"name\": \"c\", \"type\": \"double\" },"
    "    { \"name\": \"d\", \"type\": { \"type\": \"array\", \"items\": \"double\" }  },"
    "    { \"name\": \"e\", \"type\": [\"null\", \"double\"] },"
    "    { \"name\": \"f\", \"type\": "
    "       { \"type\": \"record\", \"name\": \"sub\", \"fields\": ["
    "           { \"name\": \"sub_a\", \"type\": \"double\" }"
    "       ]}"
    "    },"
    "    { \"name\": \"g\", \"type\": \"string\", \"default\": \"default g\" },"
    "    { \"name\": \"h\", \"type\": [\"string\", \"float\"], \"default\": \"default h\"  },"
    "    { \"name\": \"i\", \"type\": [\"null\", \"float\"], \"default\": null },"
    "    { \"name\": \"j\", \"type\": \"bytes\"}"
    "  ]"
    "}";

    float expected = 62.0;
    const char* file_name = "test_resolution1.dat";

    // Float schema value
    avro_schema_error_t  error;
    avro_value_t in_val;
    avro_schema_t in_schema;
    avro_schema_from_json(in_schema_json, 0, &in_schema, &error);


    avro_file_writer_t  file;
    avro_file_writer_create(file_name, in_schema, &file);
    avro_value_iface_t  *in_class = avro_generic_class_from_schema(in_schema);
    avro_generic_value_new(in_class, &in_val);

    // In record
    avro_value_t  field;
    avro_value_get_by_name(&in_val, "a", &field, NULL);
    avro_value_set_int(&field, 1);

    avro_value_get_by_name(&in_val, "b", &field, NULL);
    avro_value_set_float(&field, 2.0);

    avro_value_get_by_name(&in_val, "c", &field, NULL);
    avro_value_set_double(&field, 3.0);

    // array
    avro_value_get_by_name(&in_val, "d", &field, NULL);
    int  j;
    for (j = 0; j < 3; j++)
    {
        avro_value_t  element;
        size_t  new_index;
        avro_value_append(&field, &element, &new_index);
        avro_value_set_int(&element, j);
    }

    // union
    avro_value_get_by_name(&in_val, "e", &field, NULL);
    avro_value_t branch;
    avro_value_get_current_branch(&field, &branch);
    avro_value_set_branch(&field, 0, &branch);
    avro_value_set_null(&branch);

    avro_value_set_branch(&field, 1, &branch);
    avro_value_set_float(&branch, 5.0);

    // record
    avro_value_get_by_name(&in_val, "f", &field, NULL);
    avro_value_t sub_field;
    avro_value_get_by_name(&field, "sub_a", &sub_field, NULL);
    avro_value_set_int(&sub_field, 6);

    // bytes
    avro_value_get_by_name(&in_val, "j", &field, NULL);
    char bytes[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    avro_value_set_bytes(&field, bytes, sizeof(bytes));

    avro_file_writer_append_value(file, &in_val);
    avro_file_writer_close(file);
    fprintf(stderr, "Values written!  Reading...\n");

    avro_file_reader_t  file_read;
    avro_file_reader(file_name, &file_read);
    // Double schema value
    avro_value_t out_val;
    avro_schema_t out_schema;
    int er = avro_schema_from_json(out_schema_json, 0, &out_schema, &error);
    if(er != 0)
    {
        fprintf(stderr, "Schema not created\n");
    }

    avro_value_iface_t  *out_class = avro_generic_class_from_schema(out_schema);
    avro_generic_value_new(out_class, &out_val);

    while (avro_file_reader_read_value_with_resolution(file_read, &in_val, &out_val) == 0)
    {
        fprintf(stderr, "Reading record!\n");
        avro_value_t field;
        avro_value_get_by_name(&out_val, "a", &field, NULL);
        float int_val = -1;
        avro_value_get_float(&field, &int_val);
        fprintf(stderr, " a: %f \n", int_val);
        CHECK_CASE(int_val, (float) 1, "Promote int to float failed.");

        avro_value_get_by_name(&out_val, "b", &field, NULL);
        double double_val = -1;
        avro_value_get_double(&field, &double_val);
        fprintf(stderr, " b: %f \n", double_val);
        CHECK_CASE(double_val, (double) 2.0, "Promote float to double failed.");

        avro_value_get_by_name(&out_val, "c", &field, NULL);
        avro_value_get_double(&field, &double_val);
        fprintf(stderr, " c: %f \n", double_val);
        CHECK_CASE(double_val, 3, "Double value resolution failed");

        avro_value_get_by_name(&out_val, "d", &field, NULL);
        size_t  actual_size = 0;
        avro_value_get_size(&field, &actual_size);
        int i = 0;
        for(i = 0; i<actual_size; ++i)
        {
            avro_value_t element;
            avro_value_get_by_index(&field, i, &element, NULL);
            double element_value = -1;
            avro_value_get_double(&element, &element_value);
            fprintf(stderr, " item %d: %f | ", i, element_value);
            CHECK_CASE(element_value, (double) i, "Array resolution failed");
        }
        fprintf(stderr, "\n");

        avro_value_get_by_name(&out_val, "e", &field, NULL);
        avro_value_t  branch;
        avro_value_get_current_branch(&field, &branch);
        double branch_value = -1;
        avro_value_get_double(&branch, &branch_value);
        fprintf(stderr, " e: %f\n", branch_value);
        CHECK_CASE(branch_value, (double) 5, "Union resolution failed");

        avro_value_get_by_name(&out_val, "f", &field, NULL);
        avro_value_t  sub_field;
        avro_value_get_by_name(&field, "sub_a", &sub_field, NULL);
        double sub_double_val = -1;
        avro_value_get_double(&sub_field, &sub_double_val);
        fprintf(stderr, " f:sub_a: %f \n", sub_double_val);
        CHECK_CASE(sub_double_val, (double) 6, "Nested record resolution failed");

        avro_value_get_by_name(&out_val, "g", &field, NULL);
        char* str_val;
        size_t sz = 0;
        avro_value_get_string(&field, &str_val, &sz);
        fprintf(stderr, " g: %s \n", str_val);
        CHECK_STRING_CASE(str_val, "default g", "Default value resolution failed");

        avro_value_get_by_name(&out_val, "h", &field, NULL);
        avro_value_t  branch_str;
        avro_value_get_current_branch(&field, &branch_str);
        char* branch_value_str;
        size_t str_sz = 0;
        avro_value_get_string(&branch_str, &branch_value_str, &str_sz);
        fprintf(stderr, " h: %s\n", branch_value);
        CHECK_STRING_CASE(branch_value_str, "default h", "Union resolution failed");

        avro_value_get_by_name(&out_val, "i", &field, NULL);
        avro_value_t  branch_null;
        avro_value_get_current_branch(&field, &branch_null);
        CHECK_CASE(avro_value_get_null(&branch_null), 0, "Cannot get null value!");

        const void* actual_buf = NULL;
        size_t actual_siz = 0;
        avro_value_get_by_name(&out_val, "j", &field, NULL);
        avro_value_get_bytes(&field, &actual_buf, &actual_size);

        if (actual_size != sizeof(bytes))
        {
            fprintf(stderr, "Unexpected bytes size\n");
            return EXIT_FAILURE;
        }

        if(memcmp(actual_buf, bytes, actual_size) != 0)
        {
            fprintf(stderr, "Unexpected bytes contents\n");
            return EXIT_FAILURE;
        }

    }
    avro_file_reader_close(file_read);
    return 0;
}


int main(void)
{
    fprintf(stderr, "**** Running Schema Resolutions test ****\n");
    return test_resolution();
}
