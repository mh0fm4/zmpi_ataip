/*
 *  Copyright (C) 2013, 2014, 2015, 2016, 2017, 2018 Michael Hofmann, Chemnitz University of Technology
 *  
 *  This file is part of the ZMPI All-to-all In-place Library.
 *  
 *  The ZMPI-ATAIP is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  The ZMPI-ATAIP is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "dash_exec.h"
#include "dash_core.h"
#include "prx.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif


#define PROC_ID2RANK(_id_, _rs_)  ((int) ((_rs_)?(_rs_)[_id_]:(_id_)))


dsint_t ds_exec_create(ds_exec_t *exec) /* ds_func ds_exec_create */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  exec->naddrs = 0;

  exec->cxt = NULL;

  exec->pre_run = NULL;
  exec->post_run = NULL;
  exec->make = NULL;
  exec->move = NULL;

#ifdef DASH_SYMMETRIC
  exec->make_sym = DASH_EXEC_MAKE_SYM_DEFAULT;
  exec->sendrecv_replace = NULL;
# ifdef DASH_SYMMETRIC_AUX
  exec->sendrecv_aux_setup = NULL;
  exec->sendrecv_aux = NULL;
  exec->sendrecv_aux_intermediate = NULL;
  exec->sendrecv_aux_finish = NULL;
# endif
#endif

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_exec_destroy(ds_exec_t *exec) /* ds_func ds_exec_destroy */
{
  return 0;
}


dsint_t ds_exec_add_address(ds_exec_t *exec, void *addr) /* ds_func ds_exec_add_address */
{
  dsint_t addr_id = -1;

  Z_TRACE_IF(DS_TRACE_IF, "START");

  if (exec->naddrs < DASH_MAX_NBUFFERS)
  {
    addr_id = exec->naddrs;
    ++exec->naddrs;

    exec->addrs[addr_id] = addr;
  }

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return addr_id;
}


#ifdef DASH_SYMMETRIC

#ifdef DASH_SYMMETRIC_AUX


typedef struct _sym_aux_t
{
  dsint_t offset;

} sym_aux_t;


static void sym_aux_setup(ds_exec_t *exec, sym_aux_t *sym_aux)
{
  if (exec->sendrecv_aux_setup) exec->sendrecv_aux_setup(exec, exec->ds->comm_size);

  sym_aux->offset = 0;
}


static void sym_aux_intermediate(ds_exec_t *exec, sym_aux_t *sym_aux)
{
  if (exec->sendrecv_aux_intermediate) exec->sendrecv_aux_intermediate(exec);

  sym_aux->offset = 0;
}


static dsint_t sym_aux_sendrecv(ds_exec_t *exec, sym_aux_t *sym_aux, int proc)
{
  if (sym_aux->offset + exec->ds->syms.counts[proc] > exec->ds->sched->aux.buf_size) return 0;

  if (!exec->sendrecv_aux(exec, proc, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[proc], exec->ds->syms.displs[proc], exec->ds->syms.counts[proc], exec->ds->syms.exec_id, exec->ds->sched->aux.buf_id, sym_aux->offset))
  {
    sym_aux_intermediate(exec, sym_aux);

    return sym_aux_sendrecv(exec, sym_aux, proc);
  }

  sym_aux->offset += exec->ds->syms.counts[proc];

  return 1;
}


static void sym_aux_finish(ds_exec_t *exec, sym_aux_t *sym_aux)
{
  sym_aux_intermediate(exec, sym_aux);

  if (exec->sendrecv_aux_finish) exec->sendrecv_aux_finish(exec);
}


#endif /* DASH_SYMMETRIC_AUX */


#define SYM_SPARSE_THRESHOLD  0.5

static void sym_prepare_full(ds_exec_t *exec)
{
  dsint_t i, j, t_dsint;
  dspint_t t_dspint;


  Z_TRACE_IF(DS_TRACE_IF, "original syms (%" dsint_fmt ")", exec->ds->syms.n);
  for (i = 0; i < exec->ds->syms.n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": rank: %d, count: %" dspint_fmt ", displ: %" dspint_fmt, i, (exec->ds->syms.proc_ids[i] >= 0)?PROC_ID2RANK(exec->ds->syms.proc_ids[i], exec->ds->syms.ranks):-1, exec->ds->syms.counts[i], exec->ds->syms.displs[i]);

  Z_TRACE_IF(DS_TRACE_IF, "mode: %s", (exec->ds->syms.nmax < exec->ds->comm_size || exec->ds->syms.n < SYM_SPARSE_THRESHOLD * exec->ds->comm_size)?"sparse":"full");

  /* skip full mode if nmax too small (i.e., sparse!!!) or only "few" sym-exchanges in comparison to comm_size (i.e., sorting the few sym-exchanges is faster than full iteration) */
  if (exec->ds->syms.nmax < exec->ds->comm_size || exec->ds->syms.n < SYM_SPARSE_THRESHOLD * exec->ds->comm_size) return;

  /* fill up with empty sym-exchanges */
  while (exec->ds->syms.n < exec->ds->comm_size)
  {
    exec->ds->syms.buf_ids[exec->ds->syms.n] = 0;
    exec->ds->syms.counts[exec->ds->syms.n] = 0;
    exec->ds->syms.displs[exec->ds->syms.n] = 0;
    exec->ds->syms.proc_ids[exec->ds->syms.n] = -1;

    ++exec->ds->syms.n;
  }

  Z_TRACE_IF(DS_TRACE_IF, "expanded syms (%" dsint_fmt ")", exec->ds->syms.n);
  for (i = 0; i < exec->ds->syms.n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": rank: %d, count: %" dspint_fmt ", displ: %" dspint_fmt, i, (exec->ds->syms.proc_ids[i] >= 0)?PROC_ID2RANK(exec->ds->syms.proc_ids[i], exec->ds->syms.ranks):-1, exec->ds->syms.counts[i], exec->ds->syms.displs[i]);

  /* permute sym-exchanges */
  if (exec->ds->syms.ranks)
  {
    for (i = 0; i < exec->ds->syms.n; ++i)
    {
      j = exec->ds->syms.proc_ids[i];

      if (j >= 0)
      {
        j = exec->ds->syms.ranks[j];

        while (j != i)
        {
          z_swap(exec->ds->syms.buf_ids[i], exec->ds->syms.buf_ids[j], t_dsint);
          z_swap(exec->ds->syms.counts[i], exec->ds->syms.counts[j], t_dspint);
          z_swap(exec->ds->syms.displs[i], exec->ds->syms.displs[j], t_dspint);
          z_swap(exec->ds->syms.proc_ids[i], exec->ds->syms.proc_ids[j], t_dspint);

          /* break if j < i since swapping entries i and j with j < i always makes entry i empty (because all entries less than i are either done (i.e., no swap) or empty) */
          if (j < i) break;

          j = exec->ds->syms.proc_ids[i];

          /* break on empty entry */
          if (j < 0) break;

          j = exec->ds->syms.ranks[j];
        }
      }

      /* override the -1 empty markers */
      exec->ds->syms.proc_ids[i] = i;
    }

    exec->ds->syms.ranks = NULL;

  } else
  {
    for (i = 0; i < exec->ds->syms.n; ++i)
    {
      j = exec->ds->syms.proc_ids[i];

      if (j >= 0)
      {
        while (j != i)
        {
          z_swap(exec->ds->syms.buf_ids[i], exec->ds->syms.buf_ids[j], t_dsint);
          z_swap(exec->ds->syms.counts[i], exec->ds->syms.counts[j], t_dspint);
          z_swap(exec->ds->syms.displs[i], exec->ds->syms.displs[j], t_dspint);
          z_swap(exec->ds->syms.proc_ids[i], exec->ds->syms.proc_ids[j], t_dspint);

          /* break if j < i since swapping entries i and j with j < i always makes entry i empty (because all entries less than i are either done (i.e., no swap) or empty) */
          if (j < i) break;

          j = exec->ds->syms.proc_ids[i];

          /* break on empty entry */
          if (j < 0) break;
        }
      }

      /* override the -1 empty markers */
      exec->ds->syms.proc_ids[i] = i;
    }
  }

  Z_TRACE_IF(DS_TRACE_IF, "permuted syms (%" dsint_fmt ")", exec->ds->syms.n);
  for (i = 0; i < exec->ds->syms.n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": rank: %d, count: %" dspint_fmt ", displ: %" dspint_fmt, i, (exec->ds->syms.proc_ids[i] >= 0)?PROC_ID2RANK(exec->ds->syms.proc_ids[i], exec->ds->syms.ranks):-1, exec->ds->syms.counts[i], exec->ds->syms.displs[i]);
}


static void sym_order_destroy(dsint_t *proc_ids)
{
  z_free(proc_ids);
}


static dsint_t *sym_order_linear_create(ds_exec_t *exec, dsint_t randomize_ranks)
{
  dsint_t i, t;
  dsint_t *order;
  prx_enumerate_t random_enumerate = PRX_ENUMERATE_NULL;
  DASH_RADIXSORT_SORT_DECLARE(dsint_t)
  dsint_t rhigh;


  order = z_alloc(exec->ds->syms.n, sizeof(dsint_t));

  if (exec->ds->syms.n >= exec->ds->comm_size)
  {
    /* full mode (i.e., stored rank-wise) */

    if (randomize_ranks)
    {
      prx_seed(2501);
      prx_enumerate_create(&random_enumerate, exec->ds->comm_size, PRX_FISHER_YATES_SHUFFLE);

      for (i = 0; i < exec->ds->syms.n; ++i)
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (exec->ds->comm_rank + (_i_)) % exec->ds->comm_size))
        order[GET(i)] = i;
#undef GET
      }

      prx_enumerate_destroy(&random_enumerate);

    } else
    {
      for (i = 0; i < exec->ds->syms.n; ++i)
      {
#define GET(_i_)  ((int) ((exec->ds->comm_rank + (_i_)) % exec->ds->comm_size))
        order[GET(i)] = i;
#undef GET
      }
    }

  } else
  {
    /* sparse mode (i.e., need to sort) */

    for (i = 0; i < exec->ds->syms.n; ++i) order[i] = i;

#define XCHG(_i_, _j_)  Z_MOP(t = order[_i_]; order[_i_] = order[_j_]; order[_j_] = t;)

    if (randomize_ranks)
    {
      prx_seed(2501);
      prx_enumerate_create(&random_enumerate, exec->ds->comm_size, PRX_FISHER_YATES_SHUFFLE);

      rhigh = (dsint_t) floor(log(exec->ds->comm_size) / log(2));

      if (exec->ds->syms.ranks)
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (exec->ds->comm_rank + exec->ds->syms.ranks[exec->ds->syms.proc_ids[order[_i_]]]) % exec->ds->comm_size))
        DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
      } else
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (exec->ds->comm_rank + exec->ds->syms.proc_ids[order[_i_]]) % exec->ds->comm_size))
        DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
      }

      prx_enumerate_destroy(&random_enumerate);

    } else
    {
      rhigh = (dsint_t) floor(log(exec->ds->comm_size) / log(2));

      if (exec->ds->syms.ranks)
      {
#define GET(_i_)  ((int) ((exec->ds->comm_rank + exec->ds->syms.ranks[exec->ds->syms.proc_ids[order[_i_]]]) % exec->ds->comm_size))
        DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
      } else
      {
#define GET(_i_)  ((int) ((exec->ds->comm_rank + exec->ds->syms.proc_ids[order[_i_]]) % exec->ds->comm_size))
        DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
      }
    }

#undef XCHG
  }

  Z_TRACE_IF(DS_TRACE_IF, "linear order:");
  for (i = 0; i < exec->ds->syms.n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt " -> %" dsint_fmt " -> %d", i, order[i], (order[i] >= 0)?PROC_ID2RANK(exec->ds->syms.proc_ids[order[i]], exec->ds->syms.ranks):-1);

  return order;
}


static void make_sym_linear(ds_exec_t *exec, dsint_t randomize)
{
  dsint_t i, j;
  int r;
  dsint_t *order;
#ifdef DASH_SYMMETRIC_AUX
  sym_aux_t sym_aux;
  dsint_t sym_aux_success;
#endif


  order = sym_order_linear_create(exec, randomize);

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_setup(exec, &sym_aux);
#endif

  for (i = 0; i < exec->ds->syms.n; ++i)
  {
    j = order[i];

    if (j < 0) continue;

    r = PROC_ID2RANK(exec->ds->syms.proc_ids[j], exec->ds->syms.ranks);

    Z_TRACE_IF(DS_TRACE_IF, "sendrecv_replace with %d", r);

    if (exec->ds->comm_rank != r && exec->ds->syms.counts[j] > 0)
    {
#ifdef DASH_SYMMETRIC_AUX
      sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
      if (!sym_aux_success)
      {
#endif
        exec->sendrecv_replace(exec, r, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
        sym_aux_intermediate(exec, &sym_aux);
# endif
      }
#endif
    }
  }

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_finish(exec, &sym_aux);
#endif

  sym_order_destroy(order);
}


#ifdef OLD_HIERARCHIC


static void make_sym_hierarchic(ds_exec_t *exec, dsint_t randomize)
{
  int low, high, mid, skip, i, j, n;
#ifdef DASH_SYMMETRIC_AUX
  sym_aux_t sym_aux;
  dsint_t sym_aux_success;
#endif
  prx_enumerate_t random_enumerate = PRX_ENUMERATE_NULL;


  low = 0;
  high = exec->ds->comm_size;

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_setup(exec, &sym_aux);
#endif

  while (low + 1 < high)
  {
    mid = (low + high) / 2;

#if 0

    if (exec->ds->comm_rank < mid)
    {
      n = high - mid;

      for (i = 0; i < n; ++i)
      {
        skip = z_min(exec->ds->comm_rank - low, n - 1);
        j = mid + (i + skip) % n;

        if (exec->ds->syms.counts[j] > 0)
        {
#ifdef DASH_SYMMETRIC_AUX
          sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
          if (!sym_aux_success)
          {
#endif
            exec->sendrecv_replace(exec, j, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
            sym_aux_intermediate(exec, &sym_aux);
# endif
          }
#endif
        }
      }

      high = mid;

    } else
    {
      n = mid - low;

      for (i = 0; i < n; ++i)
      {
        skip = z_min(exec->ds->comm_rank - mid, n - 1);
        j = low + (skip - i + n) % n;

        if (exec->ds->syms.counts[j] > 0)
        {
#ifdef DASH_SYMMETRIC_AUX
          sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
          if (!sym_aux_success)
          {
#endif
            exec->sendrecv_replace(exec, j, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
            sym_aux_intermediate(exec, &sym_aux);
# endif
          }
#endif
        }
      }

      low = mid;
    }

#else

    n = z_max(mid - low, high - mid);

    if (randomize)
    {
      prx_seed(2501);
      prx_enumerate_create(&random_enumerate, n, PRX_FISHER_YATES_SHUFFLE);
    }

    if (exec->ds->comm_rank < mid)
    {
      for (i = 0; i < n; ++i)
      {
        skip = exec->ds->comm_rank - low;
        j = mid + ((randomize?prx_enumerate(random_enumerate, i):i) + skip) % n;

        if (j >= mid && exec->ds->syms.counts[j] > 0)
        {
#ifdef DASH_SYMMETRIC_AUX
          sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
          if (!sym_aux_success)
          {
#endif
            exec->sendrecv_replace(exec, j, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
            sym_aux_intermediate(exec, &sym_aux);
# endif
          }
#endif
        }
      }

      high = mid;

    } else
    {
      for (i = 0; i < n; ++i)
      {
        skip = exec->ds->comm_rank - mid;
        j = low + (skip - (randomize?prx_enumerate(random_enumerate, i):i) + n) % n;

        if (j < mid && exec->ds->syms.counts[j] > 0)
        {
#ifdef DASH_SYMMETRIC_AUX
          sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
          if (!sym_aux_success)
          {
#endif
            exec->sendrecv_replace(exec, j, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
            sym_aux_intermediate(exec, &sym_aux);
# endif
          }
#endif
        }
      }

      low = mid;
    }

    if (randomize) prx_enumerate_destroy(&random_enumerate);
#endif
  }

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_finish(exec, &sym_aux);
#endif
}


#else /* OLD_HIERARCHIC */


static dsint_t *sym_order_hierarchic_create(ds_exec_t *exec, dsint_t randomize_ranks)
{
  dsint_t i, t;
  dsint_t *order;
  DASH_RADIXSORT_SORT_DECLARE(dsint_t)
  dsint_t rhigh;


  if (exec->ds->syms.n >= exec->ds->comm_size)
  {
    /* full mode (i.e., stored rank-wise, no need for separate rank order) */

    order = z_alloc(exec->ds->syms.n, sizeof(dsint_t));

  } else
  {
    /* sparse mode (i.e., need to sort to create rank order) */

    order = z_alloc(2 * exec->ds->syms.n + 2, sizeof(dsint_t));

    for (i = 0; i < exec->ds->syms.n; ++i) order[i] = i;

#define XCHG(_i_, _j_)  Z_MOP(t = order[_i_]; order[_i_] = order[_j_]; order[_j_] = t;)

    rhigh = (dsint_t) floor(log(exec->ds->comm_size) / log(2));

    if (exec->ds->syms.ranks)
    {
#define GET(_i_)  ((int) exec->ds->syms.ranks[exec->ds->syms.proc_ids[order[_i_]]])
      DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
    } else
    {
#define GET(_i_)  ((int) exec->ds->syms.proc_ids[order[_i_]])
      DASH_RADIXSORT_SORT(exec->ds->syms.n, 0, rhigh, GET, XCHG);
#undef GET
    }

#undef XCHG

    /* store range for next search in select */
    order[2 * exec->ds->syms.n + 0] = 0;
    order[2 * exec->ds->syms.n + 1] = exec->ds->syms.n;
  }

  return order;
}


static dsint_t sym_order_hierarchic_search_rank_ge(ds_exec_t *exec, dsint_t *order, int rank, dsint_t range_low, dsint_t range_high)
{
  DASH_BINARY_SEARCH_DECLARE()
  dsint_t i;


  Z_TRACE_IF(DS_TRACE_IF, "search rank ge %d in [%" dsint_fmt ",%" dsint_fmt "]", rank, range_low, range_high);
  for (i = range_low; i < range_high; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": %" dsint_fmt " -> %" dspint_fmt, i, order[i], exec->ds->syms.proc_ids[order[i]]);

  --range_high;

  if (exec->ds->syms.ranks)
  {
#define GET(_i_)  exec->ds->syms.ranks[exec->ds->syms.proc_ids[order[_i_]]]
    DASH_BINARY_SEARCH_GE(range_low, range_high, rank, GET, i);
#undef GET

  } else
  {
#define GET(_i_)  exec->ds->syms.proc_ids[order[_i_]]
    DASH_BINARY_SEARCH_GE(range_low, range_high, rank, GET, i);
#undef GET
  }

  Z_TRACE_IF(DS_TRACE_IF, "found %" dsint_fmt, i);

  return i;
}


static dsint_t sym_order_hierarchic_search_rank_le(ds_exec_t *exec, dsint_t *order, int rank, dsint_t range_low, dsint_t range_high)
{
  DASH_BINARY_SEARCH_DECLARE()
  dsint_t i;


  Z_TRACE_IF(DS_TRACE_IF, "search rank le %d in [%" dsint_fmt ",%" dsint_fmt "]", rank, range_low, range_high);
  for (i = range_low; i < range_high; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": %" dsint_fmt " -> %" dspint_fmt, i, order[i], exec->ds->syms.proc_ids[order[i]]);

  --range_high;

  if (exec->ds->syms.ranks)
  {
#define GET(_i_)  exec->ds->syms.ranks[exec->ds->syms.proc_ids[order[_i_]]]
    DASH_BINARY_SEARCH_LE(range_low, range_high, rank, GET, i);
#undef GET

  } else
  {
#define GET(_i_)  exec->ds->syms.proc_ids[order[_i_]]
    DASH_BINARY_SEARCH_LE(range_low, range_high, rank, GET, i);
#undef GET
  }

  Z_TRACE_IF(DS_TRACE_IF, "found %" dsint_fmt, i);

  return i;
}


static dsint_t *sym_order_hierarchic_select(ds_exec_t *exec, dsint_t randomize_ranks, dsint_t *order, int low, int mid, int high, dsint_t *n)
{
  dsint_t i, t, l, h, xsize, obase, osize, mskip, f;
  dsint_t *select;
  prx_enumerate_t random_enumerate = PRX_ENUMERATE_NULL;
  DASH_RADIXSORT_SORT_DECLARE(dsint_t)
  dsint_t rhigh;


  Z_TRACE_IF(DS_TRACE_IF, "low = %d, mid = %d, high = %d", low, mid, high);

  select = order;

  if (exec->ds->syms.n >= exec->ds->comm_size)
  {
    /* full mode (i.e., stored rank-wise) */

    if (exec->ds->comm_rank < mid)
    {
      i = low;
      l = mid;
      h = high;

    } else
    {
      l = low;
      h = mid;
      i = high;
    }

  } else
  {
    /* sparse mode (i.e., have rank order) */

    select += exec->ds->syms.n;

    if (exec->ds->comm_rank < mid)
    {
      i = sym_order_hierarchic_search_rank_ge(exec, order, low, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);
      l = sym_order_hierarchic_search_rank_ge(exec, order, mid, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);
      h = sym_order_hierarchic_search_rank_ge(exec, order, high, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);

      order[2 * exec->ds->syms.n + 0] = i;
      order[2 * exec->ds->syms.n + 1] = l;

    } else
    {
      l = sym_order_hierarchic_search_rank_ge(exec, order, low, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);
      h = sym_order_hierarchic_search_rank_ge(exec, order, mid, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);
      i = sym_order_hierarchic_search_rank_ge(exec, order, high, order[2 * exec->ds->syms.n + 0], order[2 * exec->ds->syms.n + 1]);

      order[2 * exec->ds->syms.n + 0] = h;
      order[2 * exec->ds->syms.n + 1] = i;
    }
  }

  xsize = z_max(mid - low, high - mid);

  Z_TRACE_IF(DS_TRACE_IF, "xsize = %" dsint_fmt ", l = %" dsint_fmt ", h = %" dsint_fmt ", i = %" dsint_fmt, xsize, l, h, i);

  if (exec->ds->comm_rank < mid)
  {
    mskip = exec->ds->comm_rank - low;
    obase = mid;
    osize = high - mid;
    f = 1;

    Z_TRACE_IF(DS_TRACE_IF, "i'm %d in lower -> exchange with %" dsint_fmt " higher!", exec->ds->comm_rank - low, osize);

  } else
  {
    mskip = exec->ds->comm_rank - mid;
    obase = low;
    osize = mid - low;
    f = -1;

    Z_TRACE_IF(DS_TRACE_IF, "i'm %d in higher -> exchange with %" dsint_fmt " lower!", exec->ds->comm_rank - mid, osize);
  }

  Z_TRACE_IF(DS_TRACE_IF, "mskip = %" dsint_fmt ", obase = %" dsint_fmt ", osize = %" dsint_fmt ", f = %" dsint_fmt, mskip, obase, osize, f);

  if (exec->ds->syms.n >= exec->ds->comm_size)
  {
    /* full mode (i.e., stored rank-wise) */

    *n = xsize;

    if (randomize_ranks)
    {
      prx_seed(2501);
      prx_enumerate_create(&random_enumerate, xsize, PRX_FISHER_YATES_SHUFFLE);

      for (i = 0; i < xsize; ++i)
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (xsize + f * ((_i_) - mskip)) % xsize))
        select[GET(i)] = (i < osize)?(obase + i):-1;
#undef GET
      }

      prx_enumerate_destroy(&random_enumerate);

    } else
    {
      for (i = 0; i < xsize; ++i)
      {
#define GET(_i_)  ((int) ((xsize + f * ((_i_) - mskip)) % xsize))
        select[GET(i)] = (i < osize)?(obase + i):-1;
#undef GET
      }
    }

  } else
  {
    /* sparse mode (i.e., have rank order) */

    *n = h - l;

    Z_TRACE_IF(DS_TRACE_IF, "using order [%" dsint_fmt ",%" dsint_fmt "]", l, h);
    for (i = 0; i < exec->ds->syms.n; ++i)
      Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": %" dsint_fmt " -> %" dspint_fmt "", i, order[i], exec->ds->syms.proc_ids[order[i]]);

    if (randomize_ranks)
    {
      for (i = 0; i < *n; ++i) select[i] = order[l + i];

#define XCHG(_i_, _j_)  Z_MOP(t = select[_i_]; select[_i_] = select[_j_]; select[_j_] = t;)

      prx_seed(2501);
      prx_enumerate_create(&random_enumerate, xsize, PRX_FISHER_YATES_SHUFFLE);

      rhigh = (dsint_t) floor(log(xsize) / log(2));

      if (exec->ds->syms.ranks)
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (xsize + f * (exec->ds->syms.ranks[exec->ds->syms.proc_ids[select[_i_]]] - obase - mskip)) % xsize))
        DASH_RADIXSORT_SORT(*n, 0, rhigh, GET, XCHG);
#undef GET
      } else
      {
#define GET(_i_)  ((int) prx_enumerate(random_enumerate, (xsize + f * (exec->ds->syms.proc_ids[select[_i_]] - obase - mskip)) % xsize))
        DASH_RADIXSORT_SORT(*n, 0, rhigh, GET, XCHG);
#undef GET
      }

      prx_enumerate_destroy(&random_enumerate);

#undef XCHG

    } else
    {
      if (f > 0) t = sym_order_hierarchic_search_rank_ge(exec, order, obase + mskip, l, h);
      else t = sym_order_hierarchic_search_rank_le(exec, order, obase + mskip, l, h);

      for (i = 0; i < *n; ++i) select[i] = order[((*n + t + i * f - l) % *n) + l];
    }
  }

  Z_TRACE_IF(DS_TRACE_IF, "hierarchic select:");
  for (i = 0; i < *n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt " -> %" dsint_fmt " -> %d", i, select[i], (select[i] >= 0)?PROC_ID2RANK(exec->ds->syms.proc_ids[select[i]], exec->ds->syms.ranks):-1);

  return select;
}


static void make_sym_hierarchic(ds_exec_t *exec, dsint_t randomize)
{
  dsint_t i, j, n;
  int r, low, high, mid;
  dsint_t *order, *select;
#ifdef DASH_SYMMETRIC_AUX
  sym_aux_t sym_aux;
  dsint_t sym_aux_success;
#endif


  order = sym_order_hierarchic_create(exec, randomize);

  low = 0;
  high = exec->ds->comm_size;

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_setup(exec, &sym_aux);
#endif

  while (low + 1 < high)
  {
    mid = (low + high) / 2;

    select = sym_order_hierarchic_select(exec, randomize, order, low, mid, high, &n);

    for (i = 0; i < n; ++i)
    {
      j = select[i];

      if (j < 0) continue;

      r = PROC_ID2RANK(exec->ds->syms.proc_ids[select[i]], exec->ds->syms.ranks);

      Z_TRACE_IF(DS_TRACE_IF, "sendrecv_replace with %d", r);

      if (exec->ds->syms.counts[j] > 0)
      {
#ifdef DASH_SYMMETRIC_AUX
        sym_aux_success = sym_aux_sendrecv(exec, &sym_aux, j);
        if (!sym_aux_success)
        {
#endif
          exec->sendrecv_replace(exec, r, exec->ds->syms.exec_id, exec->ds->syms.buf_ids[j], exec->ds->syms.displs[j], exec->ds->syms.counts[j]);
#ifdef DASH_SYMMETRIC_AUX
# ifdef DASH_SYMMETRIC_AUX_IMMEDIATELY
          sym_aux_intermediate(exec, &sym_aux);
# endif
        }
#endif
      }
    }

    if (exec->ds->comm_rank < mid)
    {
      high = mid;

    } else
    {
      low = mid;
    }
  }

#ifdef DASH_SYMMETRIC_AUX
  sym_aux_finish(exec, &sym_aux);
#endif

  sym_order_destroy(order);
}


#endif /* OLD_HIERARCHIC */


static void make_sym(ds_exec_t *exec)
{
  dsint_t i;


  Z_TRACE_IF(DS_TRACE_IF, "START");

  Z_TRACE_IF(DS_TRACE_IF, "comm_size: %d, syms: max: %" dsint_fmt ", n: %" dsint_fmt, exec->ds->comm_size, exec->ds->syms.nmax, exec->ds->syms.n);
  for (i = 0; i < exec->ds->syms.n; ++i)
    Z_TRACE_IF(DS_TRACE_IF, "  %" dsint_fmt ": count: %" dspint_fmt ", displ: %" dspint_fmt ", proc_id: %" dspint_fmt, i, exec->ds->syms.counts[i], exec->ds->syms.displs[i], exec->ds->syms.proc_ids[i]);

  sym_prepare_full(exec);

  i = exec->make_sym;

switch_sym:

  switch (i)
  {
    case DASH_EXEC_MAKE_SYM_LINEAR:
      Z_TRACE_IF(DS_TRACE_IF, "make_sym_linear");
      make_sym_linear(exec, 0);
      break;
    case DASH_EXEC_MAKE_SYM_LINEAR_RANDOM:
      Z_TRACE_IF(DS_TRACE_IF, "make_sym_linear_random");
      make_sym_linear(exec, 1);
      break;
    case DASH_EXEC_MAKE_SYM_HIERARCHIC:
      Z_TRACE_IF(DS_TRACE_IF, "make_sym_hierarchic");
      make_sym_hierarchic(exec, 0);
      break;
    case DASH_EXEC_MAKE_SYM_HIERARCHIC_RANDOM:
      Z_TRACE_IF(DS_TRACE_IF, "make_sym_hierarchic_random");
      make_sym_hierarchic(exec, 1);
      break;
    default:
      Z_TRACE_IF(DS_TRACE_IF, "make_sym default");
      i = DASH_EXEC_MAKE_SYM_DEFAULT;
      goto switch_sym;
  }

  Z_TRACE_IF(DS_TRACE_IF, "END");
}


#endif /* DASH_SYMMETRIC */


dsint_t ds_exec_make(ds_exec_t *exec) /* ds_func ds_exec_make */
{
#ifdef DASH_SYMMETRIC
  if (exec->ds->syms.execute)
  {
    make_sym(exec);
    exec->ds->syms.execute = 0;
  }
#endif

  if (exec->ds->sends.execute || exec->ds->recvs.execute || exec->ds->locals.execute)
  {
    exec->make(exec);
    exec->ds->sends.execute = exec->ds->recvs.execute = exec->ds->locals.execute = 0;
  }

  return 0;
}


#undef PROC_ID2RANK

#undef DS_TRACE_IF
