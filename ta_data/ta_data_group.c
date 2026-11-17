/* SPDX-License-Identifier: MIT */
/* Copyright (C) 2026 Interpretica, Unipessoal Lda. All rights reserved. */
/** @file
 * @brief Agent-side dataset engine: hash-partitioned group-by on disk
 *
 * The one primitive behind every operation that has to match records
 * across a whole dataset: unordered and key-based comparison, duplicate
 * detection, referential checks, aggregation. Records are hashed by key
 * into spill partitions on disk, then each partition is loaded alone and
 * grouped in memory. A partition that does not fit the memory budget is
 * split again with another hash seed, so the budget holds however the
 * keys are distributed - short of a single key whose records alone do
 * not fit, which is reported as TA_DATA_E2BIG.
 *
 * Spill files are created with mkstemp() (mode 0600) and unlinked at
 * once, so nothing is left behind on the agent even when the process
 * dies mid-operation.
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ta_data_int.h"

/** Fan-out when a partition has to be split again. */
#define TD_SPLIT_FANOUT 16
/** How deep re-splitting may go before giving up. */
#define TD_SPLIT_DEPTH 6
/** Per-record in-memory overhead of the grouping, estimated. */
#define TD_REC_OVERHEAD 64

typedef struct td_part {
    FILE       *f;
    uint64_t    bytes;
    uint64_t    recs;
} td_part;

struct td_group {
    td_mem     *mem;
    char       *tmpdir;
    unsigned    nparts;
    td_part    *parts;
    unsigned    used;       /**< partitions processed, splits included */
    td_buf      rec;
};

static FILE *
spill_file(const char *tmpdir, char **err)
{
    td_buf tmpl = TD_BUF_INIT;
    FILE *f;
    int fd;

    td_buf_printf(&tmpl, "%s/ta_data_spill_XXXXXX", tmpdir);
    fd = mkstemp(tmpl.ptr);
    if (fd < 0)
    {
        td_fail(err, TA_DATA_EIO, "cannot create a spill file in '%s': %s",
                tmpdir, strerror(errno));
        td_buf_free(&tmpl);
        return NULL;
    }
    unlink(tmpl.ptr);
    td_buf_free(&tmpl);
    f = fdopen(fd, "w+b");
    if (f == NULL)
    {
        close(fd);
        td_fail(err, TA_DATA_EIO, "fdopen spill: %s", strerror(errno));
    }
    return f;
}

static unsigned
budget_parts(const td_mem *mem, uint64_t hint)
{
    uint64_t budget = mem != NULL && mem->limit != 0 ? mem->limit :
                      TD_MEM_DEFAULT;
    uint64_t n;

    /* Leave half of the budget for the grouping's own overhead. */
    n = (hint * 2) / (budget / 2 == 0 ? 1 : budget / 2) + 1;
    if (n > 256)
        n = 256;
    return (unsigned)n;
}

int
td_group_open(td_group **out, td_mem *mem, const char *tmpdir,
              uint64_t size_hint, char **err)
{
    td_group *g = calloc(1, sizeof(*g));

    if (g == NULL)
        return TA_DATA_ENOMEM;
    g->mem = mem;
    g->tmpdir = td_strdup(tmpdir == NULL || *tmpdir == '\0' ?
                          (getenv("TMPDIR") != NULL ? getenv("TMPDIR") :
                           "/tmp") : tmpdir);
    g->nparts = budget_parts(mem, size_hint);
    g->parts = calloc(g->nparts, sizeof(*g->parts));
    if (g->parts == NULL)
    {
        td_group_close(g);
        return TA_DATA_ENOMEM;
    }
    (void)err;
    *out = g;
    return 0;
}

static void
put_u32(td_buf *b, uint32_t v)
{
    unsigned char t[4] = { (unsigned char)v, (unsigned char)(v >> 8),
                           (unsigned char)(v >> 16),
                           (unsigned char)(v >> 24) };

    td_buf_add(b, t, 4);
}

static uint32_t
get_u32(const char *p)
{
    const unsigned char *u = (const unsigned char *)p;

    return (uint32_t)u[0] | (uint32_t)u[1] << 8 | (uint32_t)u[2] << 16 |
           (uint32_t)u[3] << 24;
}

static void
put_u64(td_buf *b, uint64_t v)
{
    put_u32(b, (uint32_t)v);
    put_u32(b, (uint32_t)(v >> 32));
}

static uint64_t
get_u64(const char *p)
{
    return (uint64_t)get_u32(p) | (uint64_t)get_u32(p + 4) << 32;
}

/* side(1) rowno(8) klen(4) key plen(4) payload */
static int
write_rec(td_buf *b, FILE *f, int side, uint64_t rowno, const void *key,
          size_t klen, const void *payload, size_t plen)
{
    td_buf_reset(b);
    td_buf_addc(b, (char)side);
    put_u64(b, rowno);
    put_u32(b, (uint32_t)klen);
    td_buf_add(b, key, klen);
    put_u32(b, (uint32_t)plen);
    td_buf_add(b, payload, plen);
    return fwrite(b->ptr, 1, b->len, f) == b->len ? 0 : TA_DATA_EIO;
}

int
td_group_add(td_group *g, int side, uint64_t rowno, const void *key,
             size_t klen, const void *payload, size_t plen, char **err)
{
    unsigned p = (unsigned)(td_hash64(key, klen, 0) % g->nparts);
    td_part *part = &g->parts[p];

    if (part->f == NULL)
    {
        part->f = spill_file(g->tmpdir, err);
        if (part->f == NULL)
            return TA_DATA_EIO;
    }
    if (write_rec(&g->rec, part->f, side, rowno, key, klen, payload,
                  plen) != 0)
        return td_fail(err, TA_DATA_EIO, "spill write: %s", strerror(errno));
    part->bytes += g->rec.len;
    part->recs++;
    return 0;
}

typedef struct parsed {
    int         side;
    uint64_t    rowno;
    const char *key;
    uint32_t    klen;
    const char *payload;
    uint32_t    plen;
    uint64_t    hash;
    size_t      next;       /* next record of the same key, or SIZE_MAX */
} parsed;

static int process(td_group *g, td_part *part, unsigned depth,
                   td_group_cb cb, void *ctx, char **err);

/* Re-split one partition into TD_SPLIT_FANOUT with a new seed. */
static int
resplit(td_group *g, td_part *part, unsigned depth, td_group_cb cb,
        void *ctx, char **err)
{
    td_part sub[TD_SPLIT_FANOUT];
    td_buf rec = TD_BUF_INIT;
    td_buf hdr = TD_BUF_INIT;
    unsigned i;
    int rc = 0;

    memset(sub, 0, sizeof(sub));
    rewind(part->f);
    for (;;)
    {
        char h[13];
        uint32_t klen;
        uint32_t plen;
        unsigned s;

        if (fread(h, 1, 13, part->f) != 13)
            break;
        klen = get_u32(h + 9);
        td_buf_reset(&hdr);
        if (td_buf_reserve(&hdr, klen + 4) != 0)
        {
            rc = TA_DATA_ENOMEM;
            break;
        }
        if (fread(hdr.ptr, 1, klen + 4, part->f) != klen + 4)
        {
            rc = td_fail(err, TA_DATA_EIO, "spill read truncated");
            break;
        }
        plen = get_u32(hdr.ptr + klen);
        td_buf_reset(&rec);
        if (td_buf_reserve(&rec, plen) != 0)
        {
            rc = TA_DATA_ENOMEM;
            break;
        }
        if (plen > 0 && fread(rec.ptr, 1, plen, part->f) != plen)
        {
            rc = td_fail(err, TA_DATA_EIO, "spill read truncated");
            break;
        }
        s = (unsigned)(td_hash64(hdr.ptr, klen, depth) % TD_SPLIT_FANOUT);
        if (sub[s].f == NULL)
        {
            sub[s].f = spill_file(g->tmpdir, err);
            if (sub[s].f == NULL)
            {
                rc = TA_DATA_EIO;
                break;
            }
        }
        rc = write_rec(&g->rec, sub[s].f, h[0], get_u64(h + 1), hdr.ptr, klen,
                       rec.ptr, plen);
        if (rc != 0)
            break;
        sub[s].bytes += g->rec.len;
        sub[s].recs++;
    }
    td_buf_free(&rec);
    td_buf_free(&hdr);
    fclose(part->f);
    part->f = NULL;
    for (i = 0; i < TD_SPLIT_FANOUT; i++)
    {
        if (rc == 0 && sub[i].f != NULL)
            rc = process(g, &sub[i], depth + 1, cb, ctx, err);
        if (sub[i].f != NULL)
            fclose(sub[i].f);
    }
    return rc;
}

static int
process(td_group *g, td_part *part, unsigned depth, td_group_cb cb,
        void *ctx, char **err)
{
    uint64_t need = part->bytes + part->recs * TD_REC_OVERHEAD;
    char *data = NULL;
    parsed *recs = NULL;
    size_t *table = NULL;
    size_t *groups = NULL;
    size_t *tail = NULL;
    td_rec *batch = NULL;
    size_t nrec = (size_t)part->recs;
    size_t tsize = 1;
    size_t ngroups = 0;
    size_t i;
    const char *p;
    int rc = 0;

    if (part->f == NULL || part->recs == 0)
        return 0;
    if (!td_mem_take(g->mem, need))
    {
        if (depth >= TD_SPLIT_DEPTH || part->recs <= 1)
            return td_fail(err, TA_DATA_E2BIG,
                           "a key group of %llu records (%llu bytes) does "
                           "not fit mem_limit=%llu",
                           (unsigned long long)part->recs,
                           (unsigned long long)need,
                           (unsigned long long)g->mem->limit);
        return resplit(g, part, depth, cb, ctx, err);
    }
    g->used++;
    data = malloc(part->bytes + 1);
    recs = calloc(nrec, sizeof(*recs));
    while (tsize < nrec * 2)
        tsize <<= 1;
    table = malloc(tsize * sizeof(*table));
    groups = malloc(nrec * sizeof(*groups));
    tail = malloc(nrec * sizeof(*tail));
    batch = malloc(nrec * sizeof(*batch));
    if (data == NULL || recs == NULL || table == NULL || groups == NULL ||
        tail == NULL || batch == NULL)
    {
        rc = TA_DATA_ENOMEM;
        goto out;
    }
    rewind(part->f);
    if (fread(data, 1, part->bytes, part->f) != part->bytes)
    {
        rc = td_fail(err, TA_DATA_EIO, "spill read: %s", strerror(errno));
        goto out;
    }
    memset(table, 0xff, tsize * sizeof(*table));
    p = data;
    for (i = 0; i < nrec; i++)
    {
        parsed *r = &recs[i];
        size_t slot;

        r->side = p[0];
        r->rowno = get_u64(p + 1);
        r->klen = get_u32(p + 9);
        r->key = p + 13;
        r->plen = get_u32(r->key + r->klen);
        r->payload = r->key + r->klen + 4;
        p = r->payload + r->plen;
        r->hash = td_hash64(r->key, r->klen, 0x9a3b);
        r->next = SIZE_MAX;
        slot = (size_t)r->hash & (tsize - 1);
        for (;;)
        {
            size_t gi = table[slot];
            parsed *first;

            if (gi == SIZE_MAX)
            {
                table[slot] = ngroups;
                groups[ngroups] = i;
                tail[ngroups] = i;
                ngroups++;
                break;
            }
            first = &recs[groups[gi]];
            if (first->hash == r->hash && first->klen == r->klen &&
                memcmp(first->key, r->key, r->klen) == 0)
            {
                recs[tail[gi]].next = i;
                tail[gi] = i;
                break;
            }
            slot = (slot + 1) & (tsize - 1);
        }
    }
    for (i = 0; i < ngroups && rc == 0; i++)
    {
        size_t n = 0;
        size_t k;

        for (k = groups[i]; k != SIZE_MAX; k = recs[k].next)
        {
            batch[n].side = recs[k].side;
            batch[n].rowno = recs[k].rowno;
            batch[n].payload = recs[k].payload;
            batch[n].plen = recs[k].plen;
            n++;
        }
        rc = cb(ctx, recs[groups[i]].key, recs[groups[i]].klen, batch, n,
                err);
    }

out:
    free(data);
    free(recs);
    free(table);
    free(groups);
    free(tail);
    free(batch);
    td_mem_give(g->mem, need);
    return rc;
}

int
td_group_run(td_group *g, td_group_cb cb, void *ctx, char **err)
{
    unsigned i;
    int rc = 0;

    for (i = 0; i < g->nparts && rc == 0; i++)
    {
        if (g->parts[i].f != NULL && fflush(g->parts[i].f) != 0)
            return td_fail(err, TA_DATA_EIO, "spill flush: %s",
                           strerror(errno));
        rc = process(g, &g->parts[i], 1, cb, ctx, err);
        if (g->parts[i].f != NULL)
        {
            fclose(g->parts[i].f);
            g->parts[i].f = NULL;
        }
    }
    return rc;
}

unsigned int
td_group_partitions(const td_group *g)
{
    return g->used;
}

void
td_group_close(td_group *g)
{
    unsigned i;

    if (g == NULL)
        return;
    for (i = 0; g->parts != NULL && i < g->nparts; i++)
    {
        if (g->parts[i].f != NULL)
            fclose(g->parts[i].f);
    }
    free(g->parts);
    free(g->tmpdir);
    td_buf_free(&g->rec);
    free(g);
}
