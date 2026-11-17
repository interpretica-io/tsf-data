/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: Apache Parquet over Arrow C++
 *
 * The one C++ unit of tsf-data, behind the plain-C seam declared in
 * ta_data_core.h (ta_data_pq_*): no Test Environment header is included
 * here, and nothing C++ crosses the seam. Reading streams record batches
 * (parquet::arrow::FileReader::GetRecordBatchReader), so a file is never
 * materialised whole; writing buffers one row group of builders at a
 * time and writes it as soon as it is full.
 *
 * Logical mapping: bool; every integer width -> int; half/float/double
 * -> float; decimal128/256 with precision <= 18 -> decimal(p,s), wider
 * -> string (its exact text); string, large_string -> string; binary,
 * fixed_size_binary -> binary; date32/64 -> date; timestamp (any unit)
 * -> timestamp (microseconds, UTC); time32/64 -> string "HH:MM:SS[.f]";
 * list, large_list, fixed_size_list -> array<T>; struct -> object{...};
 * map -> object; dictionary -> its value type. Nested values travel as
 * JSON text, canonicalised on the C side.
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/decimal.h>
#include <arrow/util/config.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/properties.h>

#include "ta_data_core.h"

namespace {

char *
dup_str(const std::string &s)
{
    char *p = static_cast<char *>(std::malloc(s.size() + 1));

    if (p != nullptr)
    {
        std::memcpy(p, s.data(), s.size());
        p[s.size()] = '\0';
    }
    return p;
}

int
fail(char **err, int rc, const std::string &msg)
{
    if (err != nullptr && *err == nullptr)
        *err = dup_str(msg);
    return rc;
}

/* ---- dates -------------------------------------------------------- */

void
civil(int64_t z, int64_t *y, unsigned *m, unsigned *d)
{
    z += 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;

    *y = static_cast<int64_t>(yoe) + era * 400;
    *d = doy - (153 * mp + 2) / 5 + 1;
    *m = mp < 10 ? mp + 3 : mp - 9;
    *y += *m <= 2;
}

std::string
fmt_date(int64_t days)
{
    int64_t y;
    unsigned m;
    unsigned d;
    char b[32];

    civil(days, &y, &m, &d);
    std::snprintf(b, sizeof(b), "%04lld-%02u-%02u",
                  static_cast<long long>(y), m, d);
    return b;
}

int64_t
floor_div(int64_t a, int64_t b)
{
    int64_t q = a / b;

    if ((a % b != 0) && ((a < 0) != (b < 0)))
        q--;
    return q;
}

std::string
fmt_ts(int64_t us)
{
    int64_t secs = floor_div(us, 1000000);
    int64_t frac = us - secs * 1000000;
    int64_t days = floor_div(secs, 86400);
    int64_t rem = secs - days * 86400;
    char b[48];

    std::string s = fmt_date(days);
    std::snprintf(b, sizeof(b), "T%02d:%02d:%02d",
                  static_cast<int>(rem / 3600),
                  static_cast<int>(rem / 60 % 60),
                  static_cast<int>(rem % 60));
    s += b;
    if (frac != 0)
    {
        std::snprintf(b, sizeof(b), ".%06lld", static_cast<long long>(frac));
        s += b;
    }
    s += 'Z';
    return s;
}

int64_t
to_us(int64_t v, arrow::TimeUnit::type unit)
{
    switch (unit)
    {
        case arrow::TimeUnit::SECOND: return v * 1000000;
        case arrow::TimeUnit::MILLI: return v * 1000;
        case arrow::TimeUnit::MICRO: return v;
        default: return floor_div(v, 1000);
    }
}

std::string
fmt_time(int64_t v, arrow::TimeUnit::type unit)
{
    int64_t us = to_us(v, unit);
    char b[40];
    int64_t s = us / 1000000;

    std::snprintf(b, sizeof(b), "%02d:%02d:%02d",
                  static_cast<int>(s / 3600), static_cast<int>(s / 60 % 60),
                  static_cast<int>(s % 60));
    std::string r = b;
    if (us % 1000000 != 0)
    {
        std::snprintf(b, sizeof(b), ".%06lld",
                      static_cast<long long>(us % 1000000));
        r += b;
    }
    return r;
}

std::string
fmt_double(double d)
{
    char b[40];

    if (std::isnan(d) || std::isinf(d))
        return "null";
    for (int prec = 15; prec <= 17; prec++)
    {
        std::snprintf(b, sizeof(b), "%.*g", prec, d);
        if (std::strtod(b, nullptr) == d)
            break;
    }
    return b;
}

void
json_escape(std::string &out, const uint8_t *p, int64_t n)
{
    out += '"';
    for (int64_t i = 0; i < n; i++)
    {
        unsigned char c = p[i];

        switch (c)
        {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20)
                {
                    char b[8];

                    std::snprintf(b, sizeof(b), "\\u%04x", c);
                    out += b;
                }
                else
                    out += static_cast<char>(c);
        }
    }
    out += '"';
}

std::string
hex(const uint8_t *p, int64_t n)
{
    static const char *const digits = "0123456789abcdef";
    std::string s = "\\x";

    for (int64_t i = 0; i < n; i++)
    {
        s += digits[p[i] >> 4];
        s += digits[p[i] & 15];
    }
    return s;
}

/* ---- types -------------------------------------------------------- */

int
logical_kind(const arrow::DataType &t, int *precision, int *scale)
{
    *precision = 0;
    *scale = 0;
    switch (t.id())
    {
        case arrow::Type::BOOL:
            return TA_DATA_T_BOOL;
        case arrow::Type::INT8: case arrow::Type::INT16:
        case arrow::Type::INT32: case arrow::Type::INT64:
        case arrow::Type::UINT8: case arrow::Type::UINT16:
        case arrow::Type::UINT32: case arrow::Type::UINT64:
            return TA_DATA_T_INT;
        case arrow::Type::HALF_FLOAT: case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
            return TA_DATA_T_FLOAT;
        case arrow::Type::DECIMAL128: case arrow::Type::DECIMAL256:
        {
            const auto &d = static_cast<const arrow::DecimalType &>(t);

            if (d.precision() > 18)
                return TA_DATA_T_STRING;
            *precision = d.precision();
            *scale = d.scale();
            return TA_DATA_T_DECIMAL;
        }
        case arrow::Type::BINARY: case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
            return TA_DATA_T_BINARY;
        case arrow::Type::DATE32: case arrow::Type::DATE64:
            return TA_DATA_T_DATE;
        case arrow::Type::TIMESTAMP:
            return TA_DATA_T_TIMESTAMP;
        case arrow::Type::LIST: case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
            return TA_DATA_T_ARRAY;
        case arrow::Type::STRUCT: case arrow::Type::MAP:
            return TA_DATA_T_OBJECT;
        case arrow::Type::DICTIONARY:
            return logical_kind(
                *static_cast<const arrow::DictionaryType &>(t).value_type(),
                precision, scale);
        default:
            return TA_DATA_T_STRING;
    }
}

std::string
type_text(const arrow::DataType &t)
{
    int p;
    int s;
    int k = logical_kind(t, &p, &s);

    switch (k)
    {
        case TA_DATA_T_BOOL: return "bool";
        case TA_DATA_T_INT: return "int";
        case TA_DATA_T_FLOAT: return "float";
        case TA_DATA_T_DECIMAL:
            return "decimal(" + std::to_string(p) + "," + std::to_string(s) +
                   ")";
        case TA_DATA_T_BINARY: return "binary";
        case TA_DATA_T_DATE: return "date";
        case TA_DATA_T_TIMESTAMP: return "timestamp";
        case TA_DATA_T_ARRAY:
        {
            const auto &l = static_cast<const arrow::BaseListType &>(
                t.id() == arrow::Type::DICTIONARY ?
                *static_cast<const arrow::DictionaryType &>(t).value_type() :
                t);

            return "array<" + type_text(*l.value_type()) + ">";
        }
        case TA_DATA_T_OBJECT:
        {
            if (t.id() != arrow::Type::STRUCT)
                return "object";
            std::string r = "object{";
            for (int i = 0; i < t.num_fields(); i++)
            {
                const auto &f = t.field(i);

                if (i > 0)
                    r += ",";
                r += "\"" + f->name() + "\":" + type_text(*f->type());
                if (!f->nullable())
                    r += "!";
            }
            return r + "}";
        }
        default:
            return "string";
    }
}

/* ---- values ------------------------------------------------------- */

void to_json(const arrow::Array &a, int64_t i, std::string &out);

std::string
decimal_text(const arrow::Array &a, int64_t i)
{
    if (a.type_id() == arrow::Type::DECIMAL128)
        return static_cast<const arrow::Decimal128Array &>(a).FormatValue(i);
    return static_cast<const arrow::Decimal256Array &>(a).FormatValue(i);
}

void
to_json(const arrow::Array &a, int64_t i, std::string &out)
{
    if (a.IsNull(i))
    {
        out += "null";
        return;
    }
    switch (a.type_id())
    {
        case arrow::Type::BOOL:
            out += static_cast<const arrow::BooleanArray &>(a).Value(i) ?
                   "true" : "false";
            return;
#define INT_CASE(_id, _cls) \
        case arrow::Type::_id:                                          \
            out += std::to_string(                                      \
                static_cast<const arrow::_cls &>(a).Value(i));          \
            return;
        INT_CASE(INT8, Int8Array)
        INT_CASE(INT16, Int16Array)
        INT_CASE(INT32, Int32Array)
        INT_CASE(INT64, Int64Array)
        INT_CASE(UINT8, UInt8Array)
        INT_CASE(UINT16, UInt16Array)
        INT_CASE(UINT32, UInt32Array)
        INT_CASE(UINT64, UInt64Array)
#undef INT_CASE
        case arrow::Type::FLOAT:
            out += fmt_double(static_cast<const arrow::FloatArray &>(a)
                              .Value(i));
            return;
        case arrow::Type::DOUBLE:
            out += fmt_double(static_cast<const arrow::DoubleArray &>(a)
                              .Value(i));
            return;
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
            out += decimal_text(a, i);
            return;
        case arrow::Type::STRING:
        {
            auto v = static_cast<const arrow::StringArray &>(a).GetView(i);

            json_escape(out, reinterpret_cast<const uint8_t *>(v.data()),
                        static_cast<int64_t>(v.size()));
            return;
        }
        case arrow::Type::LARGE_STRING:
        {
            auto v = static_cast<const arrow::LargeStringArray &>(a)
                     .GetView(i);

            json_escape(out, reinterpret_cast<const uint8_t *>(v.data()),
                        static_cast<int64_t>(v.size()));
            return;
        }
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
        {
            std::string_view v =
                a.type_id() == arrow::Type::BINARY ?
                static_cast<const arrow::BinaryArray &>(a).GetView(i) :
                a.type_id() == arrow::Type::LARGE_BINARY ?
                static_cast<const arrow::LargeBinaryArray &>(a).GetView(i) :
                static_cast<const arrow::FixedSizeBinaryArray &>(a)
                    .GetView(i);
            std::string h = hex(reinterpret_cast<const uint8_t *>(v.data()),
                                static_cast<int64_t>(v.size()));

            json_escape(out, reinterpret_cast<const uint8_t *>(h.data()),
                        static_cast<int64_t>(h.size()));
            return;
        }
        case arrow::Type::DATE32:
        {
            std::string d = fmt_date(
                static_cast<const arrow::Date32Array &>(a).Value(i));

            out += "\"" + d + "\"";
            return;
        }
        case arrow::Type::DATE64:
        {
            std::string d = fmt_date(floor_div(
                static_cast<const arrow::Date64Array &>(a).Value(i),
                86400000));

            out += "\"" + d + "\"";
            return;
        }
        case arrow::Type::TIMESTAMP:
        {
            const auto &t = static_cast<const arrow::TimestampType &>(
                *a.type());

            out += "\"" + fmt_ts(to_us(
                static_cast<const arrow::TimestampArray &>(a).Value(i),
                t.unit())) + "\"";
            return;
        }
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
        {
            std::shared_ptr<arrow::Array> v;

            if (a.type_id() == arrow::Type::LIST)
                v = static_cast<const arrow::ListArray &>(a).value_slice(i);
            else if (a.type_id() == arrow::Type::LARGE_LIST)
                v = static_cast<const arrow::LargeListArray &>(a)
                    .value_slice(i);
            else
                v = static_cast<const arrow::FixedSizeListArray &>(a)
                    .value_slice(i);
            out += '[';
            for (int64_t k = 0; k < v->length(); k++)
            {
                if (k > 0)
                    out += ',';
                to_json(*v, k, out);
            }
            out += ']';
            return;
        }
        case arrow::Type::STRUCT:
        {
            const auto &s = static_cast<const arrow::StructArray &>(a);

            out += '{';
            for (int f = 0; f < s.num_fields(); f++)
            {
                const std::string &n = s.struct_type()->field(f)->name();

                if (f > 0)
                    out += ',';
                json_escape(out, reinterpret_cast<const uint8_t *>(n.data()),
                            static_cast<int64_t>(n.size()));
                out += ':';
                to_json(*s.field(f), i, out);
            }
            out += '}';
            return;
        }
        case arrow::Type::MAP:
        {
            const auto &m = static_cast<const arrow::MapArray &>(a);
            int64_t off = m.value_offset(i);
            int64_t len = m.value_length(i);

            out += '{';
            for (int64_t k = 0; k < len; k++)
            {
                std::string key;

                if (k > 0)
                    out += ',';
                to_json(*m.keys(), off + k, key);
                if (key.empty() || key[0] != '"')
                    key = "\"" + key + "\"";
                out += key + ":";
                to_json(*m.items(), off + k, out);
            }
            out += '}';
            return;
        }
        case arrow::Type::DICTIONARY:
        {
            const auto &d = static_cast<const arrow::DictionaryArray &>(a);

            to_json(*d.dictionary(), d.GetValueIndex(i), out);
            return;
        }
        default:
        {
            auto sc = a.GetScalar(i);

            if (sc.ok())
            {
                std::string t = (*sc)->ToString();

                json_escape(out, reinterpret_cast<const uint8_t *>(t.data()),
                            static_cast<int64_t>(t.size()));
            }
            else
                out += "null";
            return;
        }
    }
}

int
set_text(ta_data_cell *c, int type, const std::string &s)
{
    c->s = dup_str(s);
    if (c->s == nullptr)
        return TA_DATA_ENOMEM;
    c->len = s.size();
    c->type = type;
    c->null = 0;
    return 0;
}

int
to_cell(const arrow::Array &a, int64_t i, ta_data_cell *c)
{
    std::memset(c, 0, sizeof(*c));
    c->null = 1;
    if (a.IsNull(i))
        return 0;
    c->null = 0;
    switch (a.type_id())
    {
        case arrow::Type::BOOL:
            c->type = TA_DATA_T_BOOL;
            c->i = static_cast<const arrow::BooleanArray &>(a).Value(i);
            return 0;
#define INT_CASE(_id, _cls) \
        case arrow::Type::_id:                                          \
            c->type = TA_DATA_T_INT;                                    \
            c->i = static_cast<int64_t>(                                \
                static_cast<const arrow::_cls &>(a).Value(i));          \
            return 0;
        INT_CASE(INT8, Int8Array)
        INT_CASE(INT16, Int16Array)
        INT_CASE(INT32, Int32Array)
        INT_CASE(INT64, Int64Array)
        INT_CASE(UINT8, UInt8Array)
        INT_CASE(UINT16, UInt16Array)
        INT_CASE(UINT32, UInt32Array)
#undef INT_CASE
        case arrow::Type::UINT64:
        {
            uint64_t v = static_cast<const arrow::UInt64Array &>(a).Value(i);

            if (v > static_cast<uint64_t>(INT64_MAX))
                return set_text(c, TA_DATA_T_STRING, std::to_string(v));
            c->type = TA_DATA_T_INT;
            c->i = static_cast<int64_t>(v);
            return 0;
        }
        case arrow::Type::HALF_FLOAT:
        {
            auto sc = a.GetScalar(i);

            c->type = TA_DATA_T_FLOAT;
            c->d = sc.ok() ? std::strtod((*sc)->ToString().c_str(), nullptr) :
                   0;
            return 0;
        }
        case arrow::Type::FLOAT:
            c->type = TA_DATA_T_FLOAT;
            c->d = static_cast<const arrow::FloatArray &>(a).Value(i);
            return 0;
        case arrow::Type::DOUBLE:
            c->type = TA_DATA_T_FLOAT;
            c->d = static_cast<const arrow::DoubleArray &>(a).Value(i);
            return 0;
        case arrow::Type::DECIMAL128:
        case arrow::Type::DECIMAL256:
        {
            const auto &t = static_cast<const arrow::DecimalType &>(*a.type());

            if (t.precision() > 18)
                return set_text(c, TA_DATA_T_STRING, decimal_text(a, i));
            /* Exact: the unscaled 128-bit value fits 64 bits here. */
            if (a.type_id() == arrow::Type::DECIMAL128)
            {
                arrow::Decimal128 v(static_cast<const arrow::Decimal128Array &>
                                    (a).GetValue(i));

                c->i = static_cast<int64_t>(v.low_bits());
            }
            else
            {
                std::string s = decimal_text(a, i);
                std::string digits;

                for (char ch : s)
                {
                    if (ch != '.')
                        digits += ch;
                }
                c->i = std::strtoll(digits.c_str(), nullptr, 10);
            }
            c->type = TA_DATA_T_DECIMAL;
            c->scale = t.scale();
            return 0;
        }
        case arrow::Type::STRING:
        {
            auto v = static_cast<const arrow::StringArray &>(a).GetView(i);

            return set_text(c, TA_DATA_T_STRING, std::string(v));
        }
        case arrow::Type::LARGE_STRING:
        {
            auto v = static_cast<const arrow::LargeStringArray &>(a)
                     .GetView(i);

            return set_text(c, TA_DATA_T_STRING, std::string(v));
        }
        case arrow::Type::BINARY:
            return set_text(c, TA_DATA_T_BINARY, std::string(
                static_cast<const arrow::BinaryArray &>(a).GetView(i)));
        case arrow::Type::LARGE_BINARY:
            return set_text(c, TA_DATA_T_BINARY, std::string(
                static_cast<const arrow::LargeBinaryArray &>(a).GetView(i)));
        case arrow::Type::FIXED_SIZE_BINARY:
            return set_text(c, TA_DATA_T_BINARY, std::string(
                static_cast<const arrow::FixedSizeBinaryArray &>(a)
                    .GetView(i)));
        case arrow::Type::DATE32:
            c->type = TA_DATA_T_DATE;
            c->i = static_cast<const arrow::Date32Array &>(a).Value(i);
            return 0;
        case arrow::Type::DATE64:
            c->type = TA_DATA_T_DATE;
            c->i = floor_div(static_cast<const arrow::Date64Array &>(a)
                             .Value(i), 86400000);
            return 0;
        case arrow::Type::TIMESTAMP:
            c->type = TA_DATA_T_TIMESTAMP;
            c->i = to_us(static_cast<const arrow::TimestampArray &>(a)
                         .Value(i),
                         static_cast<const arrow::TimestampType &>(*a.type())
                         .unit());
            return 0;
        case arrow::Type::TIME32:
            return set_text(c, TA_DATA_T_STRING, fmt_time(
                static_cast<const arrow::Time32Array &>(a).Value(i),
                static_cast<const arrow::Time32Type &>(*a.type()).unit()));
        case arrow::Type::TIME64:
            return set_text(c, TA_DATA_T_STRING, fmt_time(
                static_cast<const arrow::Time64Array &>(a).Value(i),
                static_cast<const arrow::Time64Type &>(*a.type()).unit()));
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::STRUCT:
        case arrow::Type::MAP:
        {
            std::string j;

            to_json(a, i, j);
            return set_text(c, a.type_id() == arrow::Type::STRUCT ||
                            a.type_id() == arrow::Type::MAP ?
                            TA_DATA_T_OBJECT : TA_DATA_T_ARRAY, j);
        }
        case arrow::Type::DICTIONARY:
        {
            const auto &d = static_cast<const arrow::DictionaryArray &>(a);

            return to_cell(*d.dictionary(), d.GetValueIndex(i), c);
        }
        default:
        {
            auto sc = a.GetScalar(i);

            return set_text(c, TA_DATA_T_STRING,
                            sc.ok() ? (*sc)->ToString() : std::string());
        }
    }
}

/* ---- reader ------------------------------------------------------- */

struct pq_reader {
    std::unique_ptr<parquet::arrow::FileReader>     file;
    std::unique_ptr<arrow::RecordBatchReader>       batches;
    std::shared_ptr<arrow::Schema>                  schema;
    std::shared_ptr<arrow::RecordBatch>             batch;
    int64_t                                         pos = 0;
    int64_t                                         rows = 0;
};

/* ---- writer ------------------------------------------------------- */

struct pq_writer {
    std::shared_ptr<arrow::io::FileOutputStream>    out;
    std::unique_ptr<parquet::arrow::FileWriter>     writer;
    std::shared_ptr<arrow::Schema>                  schema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    std::vector<int>                                kinds;
    int64_t                                         group_rows = 65536;
    int64_t                                         pending = 0;
};

std::shared_ptr<arrow::DataType>
arrow_type(const ta_data_pq_col &c)
{
    switch (c.type)
    {
        case TA_DATA_T_BOOL: return arrow::boolean();
        case TA_DATA_T_INT: return arrow::int64();
        case TA_DATA_T_FLOAT: return arrow::float64();
        case TA_DATA_T_DECIMAL:
            return arrow::decimal128(c.precision < 1 ? 18 : c.precision,
                                     c.scale);
        case TA_DATA_T_BINARY: return arrow::binary();
        case TA_DATA_T_DATE: return arrow::date32();
        case TA_DATA_T_TIMESTAMP:
            return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
        default: return arrow::utf8();   /* string, and nested as JSON */
    }
}

arrow::Status
flush(pq_writer *w)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    if (w->pending == 0)
        return arrow::Status::OK();
    for (auto &b : w->builders)
    {
        std::shared_ptr<arrow::Array> a;

        ARROW_RETURN_NOT_OK(b->Finish(&a));
        arrays.push_back(a);
    }
    auto table = arrow::Table::Make(w->schema, arrays, w->pending);
    ARROW_RETURN_NOT_OK(w->writer->WriteTable(*table, w->pending));
    w->pending = 0;
    return arrow::Status::OK();
}

arrow::Status
append(pq_writer *w, size_t k, const ta_data_cell *c)
{
    arrow::ArrayBuilder *b = w->builders[k].get();

    if (c->null)
        return b->AppendNull();
    switch (w->kinds[k])
    {
        case TA_DATA_T_BOOL:
            return static_cast<arrow::BooleanBuilder *>(b)->Append(c->i != 0);
        case TA_DATA_T_INT:
            return static_cast<arrow::Int64Builder *>(b)->Append(c->i);
        case TA_DATA_T_FLOAT:
            return static_cast<arrow::DoubleBuilder *>(b)->Append(c->d);
        case TA_DATA_T_DECIMAL:
            return static_cast<arrow::Decimal128Builder *>(b)->Append(
                arrow::Decimal128(c->i));
        case TA_DATA_T_BINARY:
            return static_cast<arrow::BinaryBuilder *>(b)->Append(
                reinterpret_cast<const uint8_t *>(c->s),
                static_cast<int32_t>(c->len));
        case TA_DATA_T_DATE:
            return static_cast<arrow::Date32Builder *>(b)->Append(
                static_cast<int32_t>(c->i));
        case TA_DATA_T_TIMESTAMP:
            return static_cast<arrow::TimestampBuilder *>(b)->Append(c->i);
        default:
            return static_cast<arrow::StringBuilder *>(b)->Append(
                c->s == nullptr ? "" : std::string(c->s, c->len));
    }
}

} /* namespace */

extern "C" {

int
ta_data_pq_supported(void)
{
    return 1;
}

const char *
ta_data_pq_version(void)
{
    return ARROW_VERSION_STRING;
}

int
ta_data_pq_open(const char *path, int batch_rows, void **h, int *ncols,
                char **err)
{
    try
    {
        auto r = std::make_unique<pq_reader>();
        parquet::arrow::FileReaderBuilder builder;
        parquet::ArrowReaderProperties props;

        auto in = arrow::io::ReadableFile::Open(path);
        if (!in.ok())
            return fail(err, TA_DATA_ENOENT, std::string("cannot open '") +
                        path + "': " + in.status().ToString());
        auto st = builder.Open(*in);
        if (!st.ok())
            return fail(err, TA_DATA_ECORRUPT, std::string("'") + path +
                        "' is not a readable Parquet file: " + st.ToString());
        props.set_batch_size(batch_rows > 0 ? batch_rows : 8192);
        builder.properties(props);
        auto file = builder.Build();
        if (!file.ok())
            return fail(err, TA_DATA_ECORRUPT, file.status().ToString());
        r->file = std::move(*file);
        st = r->file->GetSchema(&r->schema);
        if (!st.ok())
            return fail(err, TA_DATA_ECORRUPT, st.ToString());
        r->rows = r->file->parquet_reader()->metadata()->num_rows();
        auto rb = r->file->GetRecordBatchReader();
        if (!rb.ok())
            return fail(err, TA_DATA_ECORRUPT, rb.status().ToString());
        r->batches = std::move(*rb);
        *ncols = r->schema->num_fields();
        *h = r.release();
        return 0;
    }
    catch (const std::exception &e)
    {
        return fail(err, TA_DATA_ECORRUPT, std::string("'") + path +
                    "': " + e.what());
    }
}

int
ta_data_pq_column(void *h, int i, ta_data_pq_col *col)
{
    auto *r = static_cast<pq_reader *>(h);
    const auto &f = r->schema->field(i);

    col->name = dup_str(f->name());
    col->type = logical_kind(*f->type(), &col->precision, &col->scale);
    col->nullable = f->nullable() ? 1 : 0;
    col->type_text = dup_str(type_text(*f->type()));
    return 0;
}

int
ta_data_pq_num_rows(void *h, int64_t *rows)
{
    *rows = static_cast<pq_reader *>(h)->rows;
    return 0;
}

int
ta_data_pq_next(void *h, ta_data_cell *cells, char **err)
{
    auto *r = static_cast<pq_reader *>(h);

    try
    {
        while (r->batch == nullptr || r->pos >= r->batch->num_rows())
        {
            std::shared_ptr<arrow::RecordBatch> b;
            auto st = r->batches->ReadNext(&b);

            if (!st.ok())
                return fail(err, TA_DATA_ECORRUPT,
                            "Parquet read: " + st.ToString());
            if (b == nullptr)
                return 1;
            r->batch = b;
            r->pos = 0;
        }
        for (int k = 0; k < r->batch->num_columns(); k++)
        {
            int rc = to_cell(*r->batch->column(k), r->pos, &cells[k]);

            if (rc != 0)
                return rc;
        }
        r->pos++;
        return 0;
    }
    catch (const std::exception &e)
    {
        return fail(err, TA_DATA_ECORRUPT, std::string("Parquet read: ") +
                    e.what());
    }
}

void
ta_data_pq_close(void *h)
{
    delete static_cast<pq_reader *>(h);
}

int
ta_data_pq_wopen(const char *path, int ncols, const ta_data_pq_col *cols,
                 int64_t group_rows, void **h, char **err)
{
    try
    {
        auto w = std::make_unique<pq_writer>();
        std::vector<std::shared_ptr<arrow::Field>> fields;

        for (int k = 0; k < ncols; k++)
        {
            auto t = arrow_type(cols[k]);
            std::unique_ptr<arrow::ArrayBuilder> b;
            auto st = arrow::MakeBuilder(arrow::default_memory_pool(), t, &b);

            if (!st.ok())
                return fail(err, TA_DATA_EINVAL, st.ToString());
            fields.push_back(arrow::field(cols[k].name, t,
                                          cols[k].nullable != 0));
            w->builders.push_back(std::move(b));
            w->kinds.push_back(cols[k].type);
        }
        w->schema = arrow::schema(fields);
        w->group_rows = group_rows > 0 ? group_rows : 65536;
        auto out = arrow::io::FileOutputStream::Open(path);
        if (!out.ok())
            return fail(err, TA_DATA_EIO, std::string("cannot create '") +
                        path + "': " + out.status().ToString());
        w->out = *out;
        auto props = parquet::WriterProperties::Builder()
                     .compression(parquet::Compression::SNAPPY)
                     ->build();
        auto fw = parquet::arrow::FileWriter::Open(
            *w->schema, arrow::default_memory_pool(), w->out, props);
        if (!fw.ok())
            return fail(err, TA_DATA_EIO, fw.status().ToString());
        w->writer = std::move(*fw);
        *h = w.release();
        return 0;
    }
    catch (const std::exception &e)
    {
        return fail(err, TA_DATA_EIO, std::string("Parquet writer: ") +
                    e.what());
    }
}

int
ta_data_pq_write(void *h, const ta_data_cell *cells, char **err)
{
    auto *w = static_cast<pq_writer *>(h);

    try
    {
        for (size_t k = 0; k < w->builders.size(); k++)
        {
            auto st = append(w, k, &cells[k]);

            if (!st.ok())
                return fail(err, TA_DATA_EIO, st.ToString());
        }
        if (++w->pending >= w->group_rows)
        {
            auto st = flush(w);

            if (!st.ok())
                return fail(err, TA_DATA_EIO, st.ToString());
        }
        return 0;
    }
    catch (const std::exception &e)
    {
        return fail(err, TA_DATA_EIO, std::string("Parquet write: ") +
                    e.what());
    }
}

int
ta_data_pq_wclose(void *h, char **err)
{
    auto *w = static_cast<pq_writer *>(h);
    int rc = 0;

    try
    {
        auto st = flush(w);

        if (st.ok())
            st = w->writer->Close();
        if (st.ok())
            st = w->out->Close();
        if (!st.ok())
            rc = fail(err, TA_DATA_EIO, st.ToString());
    }
    catch (const std::exception &e)
    {
        rc = fail(err, TA_DATA_EIO, std::string("Parquet close: ") +
                  e.what());
    }
    delete w;
    return rc;
}

} /* extern "C" */
