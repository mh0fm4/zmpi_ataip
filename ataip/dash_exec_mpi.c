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
#include "dash_exec_mpi.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif

/*#define MAKE_WITH_ALLTOALLW*/

/*#define PRINT_CONTENT*/


static dsint_t ds_exec_mpi_pre_run(ds_exec_t *exec);
static dsint_t ds_exec_mpi_post_run(ds_exec_t *exec);
#ifdef MAKE_WITH_ALLTOALLW
static dsint_t ds_exec_mpi_make_alltoallw(ds_exec_t *exec);
#else
static dsint_t ds_exec_mpi_make_isendrecv(ds_exec_t *exec);
#endif

static void ds_exec_mpi_move(ds_exec_t *exec, dsint_t exec_id, dsint_t src_buf_id, dsint_t src_displ, dsint_t dst_buf_id, dsint_t dst_displ, dsint_t count);
#ifdef DASH_SYMMETRIC
static void ds_exec_mpi_sendrecv_replace(ds_exec_t *exec, int proc, dsint_t exec_id, dsint_t buf_id, dspint_t displ, dspint_t count);
# ifdef DASH_SYMMETRIC_AUX
static void ds_exec_mpi_sendrecv_aux_setup(ds_exec_t *exec, dsint_t max_nsyms);
static dsint_t ds_exec_mpi_sendrecv_aux(ds_exec_t *exec, int proc, dsint_t exec_id, dsint_t buf_id, dspint_t displ, dspint_t count, dsint_t aux_exec_id, dsint_t aux_buf_id, dspint_t aux_displ);
static void ds_exec_mpi_sendrecv_aux_intermediate(ds_exec_t *exec);
static void ds_exec_mpi_sendrecv_aux_finish(ds_exec_t *exec);
# endif
#endif


dsint_t ds_exec_mpi_create(ds_exec_t *exec) /* ds_func ds_exec_mpi_create */
{
  ds_exec_mpi_t *exec_mpi;


  Z_TRACE_IF(DS_TRACE_IF, "START");

  ds_exec_create(exec);

  exec->cxt = exec_mpi = z_alloc(1, sizeof(ds_exec_mpi_t));

  exec->pre_run = ds_exec_mpi_pre_run;
  exec->post_run = ds_exec_mpi_post_run;
#ifdef MAKE_WITH_ALLTOALLW
  exec->make = ds_exec_mpi_make_alltoallw;
#else
  exec->make = ds_exec_mpi_make_isendrecv;
#endif
  exec->move = ds_exec_mpi_move;

#ifdef DASH_SYMMETRIC
  exec->sendrecv_replace = ds_exec_mpi_sendrecv_replace;
# ifdef DASH_SYMMETRIC_AUX
  exec->sendrecv_aux_setup = ds_exec_mpi_sendrecv_aux_setup;
  exec->sendrecv_aux = ds_exec_mpi_sendrecv_aux;
  exec->sendrecv_aux_intermediate = ds_exec_mpi_sendrecv_aux_intermediate;
  exec->sendrecv_aux_finish = ds_exec_mpi_sendrecv_aux_finish;
# endif
#endif

  exec_mpi->ntypes = 0;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_exec_mpi_destroy(ds_exec_t *exec) /* ds_func ds_exec_mpi_destroy */
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t i;


  Z_TRACE_IF(DS_TRACE_IF, "START");

  for (i = 0; i < exec_mpi->ntypes; ++i) zmpil_destroy(&exec_mpi->zmpil_types[i]);

  z_free(exec->cxt);

  ds_exec_destroy(exec);

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


static dsint_t ds_exec_mpi_pre_run(ds_exec_t *exec)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);


  Z_TRACE_IF(DS_TRACE_IF, "START");

  exec_mpi->nmax = exec->ds->sends.nmax + exec->ds->recvs.nmax;
  exec_mpi->n = 0;
  exec_mpi->reqs = z_alloc(exec_mpi->nmax, sizeof(MPI_Request));
  exec_mpi->stats = z_alloc(exec_mpi->nmax, sizeof(MPI_Status));

#ifdef MAKE_WITH_ALLTOALLW
  exec_mpi->scounts = z_alloc(4 * exec->ds->comm_size, sizeof(int));
  exec_mpi->sdispls = exec_mpi->scounts + 1 * exec->ds->comm_size;
  exec_mpi->rcounts = exec_mpi->scounts + 2 * exec->ds->comm_size;
  exec_mpi->rdispls = exec_mpi->scounts + 3 * exec->ds->comm_size;

  exec_mpi->stypes = z_alloc(2 * exec->ds->comm_size, sizeof(MPI_Datatype));
  exec_mpi->rtypes = exec_mpi->stypes + 1 * exec->ds->comm_size;

  exec_mpi->addr_types = z_alloc(exec->naddrs, sizeof(MPI_Datatype));
#endif

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


static dsint_t ds_exec_mpi_post_run(ds_exec_t *exec)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);


  Z_TRACE_IF(DS_TRACE_IF, "START");

  z_free(exec_mpi->reqs); exec_mpi->reqs = NULL;
  z_free(exec_mpi->stats); exec_mpi->stats = NULL;

#ifdef MAKE_WITH_ALLTOALLW
  z_free(exec_mpi->scounts);
  exec_mpi->scounts = NULL;
  exec_mpi->sdispls = NULL;
  exec_mpi->rcounts = NULL;
  exec_mpi->rdispls = NULL;

  z_free(exec_mpi->stypes);
  exec_mpi->stypes = NULL;
  exec_mpi->rtypes = NULL;

  z_free(exec_mpi->addr_types);
  exec_mpi->addr_types = NULL;
#endif

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


#ifdef MAKE_WITH_ALLTOALLW

static dsint_t ds_exec_mpi_make_alltoallw(ds_exec_t *exec)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t i, req_id;
  dsint_t src_buf_id, dst_buf_id;
  dsint_t src_exec_id, dst_exec_id;
  void *src_addr, *dst_addr;

  int rcount;

  MPI_Aint displ;


  Z_TRACE_IF(DS_TRACE_IF, "START");

  for (i = 0; i < exec->ds->comm_size; ++i)
  {
    exec_mpi->scounts[i] = exec_mpi->sdispls[i] = exec_mpi->rcounts[i] = exec_mpi->rdispls[i] = 0;
    exec_mpi->stypes[i] = exec_mpi->rtypes[i] = MPI_DATATYPE_NULL;
  }

  for (i = 0; i < exec->naddrs; ++i) exec_mpi->addr_types[i] = MPI_DATATYPE_NULL;

  req_id = 0;

  dst_exec_id = exec->ds->recvs.exec_id;

  for (i = 0; i < exec->ds->recvs.n; ++i)
  {
    dst_buf_id = exec->ds->recvs.buf_ids[i];
    dst_addr = exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id];

#if 0
    if (exec_mpi->addr_types[exec->ds->sched->bufs[dst_buf_id].addr_id] == MPI_DATATYPE_NULL)
    {
      MPI_Get_address(dst_addr, &displ);
      MPI_Type_create_resized(exec_mpi->mpi_types[dst_exec_id], displ1, &blockl, &displ, , &exec_mpi->addr_types[exec->ds->sched->bufs[dst_buf_id].addr_id]);
/*      MPI_Type_create_hindexed(1, &blockl, &displ, exec_mpi->mpi_types[dst_exec_id], &exec_mpi->addr_types[exec->ds->sched->bufs[dst_buf_id].addr_id]);*/
      MPI_Type_commit(&exec_mpi->addr_types[exec->ds->sched->bufs[dst_buf_id].addr_id]);
    }
#endif

    if (exec_mpi->rcounts[exec->ds->recvs.proc_ids[i]] == 0)
    {
      exec_mpi->rcounts[exec->ds->recvs.proc_ids[i]] = exec->ds->recvs.counts[i];
      MPI_Get_address(zmpil_at(dst_addr, exec->ds->recvs.displs[i], &exec_mpi->zmpil_types[dst_exec_id]), &displ);
      exec_mpi->rdispls[exec->ds->recvs.proc_ids[i]] = displ;
/*      printf("R: %" dsint_fmt " / %p / %" dspint_fmt " / %lld / %d\n", dst_buf_id, dst_addr, exec->ds->recvs.displs[i], (long long) displ, exec_mpi->rdispls[exec->ds->recvs.proc_ids[i]]);*/
      exec_mpi->rtypes[exec->ds->recvs.proc_ids[i]] = exec_mpi->mpi_types[dst_exec_id];

    } else
    {
      MPI_Irecv(zmpil_at(dst_addr, exec->ds->recvs.displs[i], &exec_mpi->zmpil_types[dst_exec_id]), exec->ds->recvs.counts[i], exec_mpi->mpi_types[dst_exec_id], exec->ds->recvs.proc_ids[i], DS_EXEC_MPI_ISENDRECV_TAG, exec->ds->comm, &exec_mpi->reqs[req_id]);

      ++req_id;
    }
  }

  src_exec_id = exec->ds->sends.exec_id;

  for (i = 0; i < exec->ds->sends.n; ++i)
  {
    src_buf_id = exec->ds->sends.buf_ids[i];
    src_addr = exec->addrs[exec->ds->sched->bufs[src_buf_id].addr_id];

    if (exec_mpi->scounts[exec->ds->sends.proc_ids[i]] == 0)
    {
      exec_mpi->scounts[exec->ds->sends.proc_ids[i]] = exec->ds->sends.counts[i];
      MPI_Get_address(zmpil_at(src_addr, exec->ds->sends.displs[i], &exec_mpi->zmpil_types[src_exec_id]), &displ);
      exec_mpi->sdispls[exec->ds->sends.proc_ids[i]] = displ;
/*      printf("S: %" dsint_fmt " / %p / %" dspint_fmt " / %lld / %d\n", src_buf_id, src_addr, exec->ds->sends.displs[i], (long long) displ, exec_mpi->sdispls[exec->ds->sends.proc_ids[i]]);*/
      exec_mpi->stypes[exec->ds->sends.proc_ids[i]] = exec_mpi->mpi_types[src_exec_id];

    } else
    {
      MPI_Isend(zmpil_at(src_addr, exec->ds->sends.displs[i], &exec_mpi->zmpil_types[src_exec_id]), exec->ds->sends.counts[i], exec_mpi->mpi_types[src_exec_id], exec->ds->sends.proc_ids[i], DS_EXEC_MPI_ISENDRECV_TAG, exec->ds->comm, &exec_mpi->reqs[req_id]);

      ++req_id;
    }
  }

  for (i = 0; i < exec->ds->locals.n; ++i)
  {
    src_buf_id = exec->ds->locals.src_buf_ids[i];
    dst_buf_id = exec->ds->locals.dst_buf_ids[i];
    src_exec_id = exec->ds->locals.src_exec_ids[i];
    dst_exec_id = exec->ds->locals.dst_exec_ids[i];
    src_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[src_buf_id].addr_id], exec->ds->locals.src_displs[i], &exec_mpi->zmpil_types[src_exec_id]);
    dst_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id], exec->ds->locals.dst_displs[i], &exec_mpi->zmpil_types[dst_exec_id]);

    /* if source and destination buffer are identical and source and destination intervals overlap */
    if (src_buf_id == dst_buf_id && z_max(exec->ds->locals.src_displs[i], exec->ds->locals.src_displs[i]) < z_min(exec->ds->locals.src_displs[i] + exec->ds->locals.src_counts[i], exec->ds->locals.src_displs[i] + exec->ds->locals.src_counts[i]))
    {
      /* move */
      zmpil_memmove_conv(dst_addr, src_addr, exec->ds->locals.src_counts[i], &exec_mpi->zmpil_types[dst_exec_id], &exec_mpi->zmpil_types[src_exec_id]);

    } else
    {
      /* copy */
      zmpil_memcpy_conv(dst_addr, src_addr, exec->ds->locals.src_counts[i], &exec_mpi->zmpil_types[dst_exec_id], &exec_mpi->zmpil_types[src_exec_id]);
    }
  }

  MPI_Alltoall(exec_mpi->scounts, 1, MPI_INT, exec_mpi->rcounts, 1, MPI_INT, exec->ds->comm);
  MPI_Alltoallw(MPI_BOTTOM, exec_mpi->scounts, exec_mpi->sdispls, exec_mpi->stypes, MPI_BOTTOM, exec_mpi->rcounts, exec_mpi->rdispls, exec_mpi->rtypes, exec->ds->comm);

  MPI_Waitall(req_id, exec_mpi->reqs, exec_mpi->stats);

  /* correct receive counts */

  req_id = 0;

  dst_exec_id = exec->ds->recvs.exec_id;

  for (i = 0; i < exec->ds->recvs.n; ++i)
  {
    if (exec_mpi->rcounts[exec->ds->recvs.proc_ids[i]] > 0)
    {
      rcount = exec_mpi->rcounts[exec->ds->recvs.proc_ids[i]];
      exec_mpi->rcounts[exec->ds->recvs.proc_ids[i]] = 0;

    } else
    {
      MPI_Get_count(&exec_mpi->stats[req_id], exec_mpi->mpi_types[dst_exec_id], &rcount);

      ++req_id;
    }

    Z_TRACE_IF(DS_TRACE_IF, "received %d from %" dspint_fmt " (expected %" dspint_fmt ")", rcount, exec->ds->recvs.proc_ids[i], exec->ds->recvs.counts[i]);

    exec->ds->recvs.counts[i] = rcount;
  }

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}

#else /* MAKE_WITH_ALLTOALLW */

static dsint_t ds_exec_mpi_make_isendrecv(ds_exec_t *exec)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t i, req_id, j;
  dsint_t src_buf_id, dst_buf_id;
  dsint_t src_exec_id, dst_exec_id;
  void *src_addr, *dst_addr;

  int rcount;

  Z_TRACE_IF(DS_TRACE_IF, "START");

  req_id = 0;

  dst_exec_id = exec->ds->recvs.exec_id;

  for (i = 0; i < exec->ds->recvs.n; ++i)
  {
    dst_buf_id = exec->ds->recvs.buf_ids[i];
    dst_addr = exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id];

    j = exec->ds->recvs.proc_ids[i];
    if (exec->ds->recvs.ranks) j = exec->ds->recvs.ranks[j];

    MPI_Irecv(zmpil_at(dst_addr, exec->ds->recvs.displs[i], &exec_mpi->zmpil_types[dst_exec_id]), exec->ds->recvs.counts[i], exec_mpi->mpi_types[dst_exec_id], j, DS_EXEC_MPI_ISENDRECV_TAG, exec->ds->comm, &exec_mpi->reqs[req_id]);

    ++req_id;
  }

  src_exec_id = exec->ds->sends.exec_id;

  for (i = 0; i < exec->ds->sends.n; ++i)
  {
    src_buf_id = exec->ds->sends.buf_ids[i];
    src_addr = exec->addrs[exec->ds->sched->bufs[src_buf_id].addr_id];

    j = exec->ds->sends.proc_ids[i];
    if (exec->ds->sends.ranks) j = exec->ds->sends.ranks[j];

    MPI_Isend(zmpil_at(src_addr, exec->ds->sends.displs[i], &exec_mpi->zmpil_types[src_exec_id]), exec->ds->sends.counts[i], exec_mpi->mpi_types[src_exec_id], j, DS_EXEC_MPI_ISENDRECV_TAG, exec->ds->comm, &exec_mpi->reqs[req_id]);

#ifdef PRINT_CONTENT
    dsint_t x;
    for (x = 0; x < exec->ds->sends.counts[i]; ++x)
      Z_TRACE_IF(DS_TRACE_IF, "send [%" dsint_fmt "]: %lld", x, ((long long *) zmpil_at(src_addr, exec->ds->sends.displs[i], &exec_mpi->zmpil_types[src_exec_id]))[x]);
#endif

    ++req_id;
  }

  for (i = 0; i < exec->ds->locals.n; ++i)
  {
    src_buf_id = exec->ds->locals.src_buf_ids[i];
    dst_buf_id = exec->ds->locals.dst_buf_ids[i];
    src_exec_id = exec->ds->locals.src_exec_ids[i];
    dst_exec_id = exec->ds->locals.dst_exec_ids[i];
    src_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[src_buf_id].addr_id], exec->ds->locals.src_displs[i], &exec_mpi->zmpil_types[src_exec_id]);
    dst_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id], exec->ds->locals.dst_displs[i], &exec_mpi->zmpil_types[dst_exec_id]);

    /* if source and destination buffer are identical and source and destination intervals overlap */
    if (src_buf_id == dst_buf_id && z_max(exec->ds->locals.src_displs[i], exec->ds->locals.src_displs[i]) < z_min(exec->ds->locals.src_displs[i] + exec->ds->locals.src_counts[i], exec->ds->locals.src_displs[i] + exec->ds->locals.src_counts[i]))
    {
      /* move */
      zmpil_memmove_conv(dst_addr, src_addr, exec->ds->locals.src_counts[i], &exec_mpi->zmpil_types[dst_exec_id], &exec_mpi->zmpil_types[src_exec_id]);

    } else
    {
      /* copy */
      zmpil_memcpy_conv(dst_addr, src_addr, exec->ds->locals.src_counts[i], &exec_mpi->zmpil_types[dst_exec_id], &exec_mpi->zmpil_types[src_exec_id]);
    }
  }

  MPI_Waitall(req_id, exec_mpi->reqs, exec_mpi->stats);

  /* correct receive counts */

  dst_exec_id = exec->ds->recvs.exec_id;

  for (i = 0; i < exec->ds->recvs.n; ++i)
  {
    MPI_Get_count(&exec_mpi->stats[i], exec_mpi->mpi_types[dst_exec_id], &rcount);

    Z_TRACE_IF(DS_TRACE_IF, "received %d from %" dspint_fmt " (expected %" dspint_fmt ")", rcount, exec->ds->recvs.proc_ids[i], exec->ds->recvs.counts[i]);

    exec->ds->recvs.counts[i] = rcount;

    dst_buf_id = exec->ds->recvs.buf_ids[i];
    dst_addr = exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id];

#ifdef PRINT_CONTENT
    dsint_t x;
    for (x = 0; x < exec->ds->recvs.counts[i]; ++x)
      Z_TRACE_IF(DS_TRACE_IF, "received [%" dsint_fmt "]: %lld", x, ((long long *) zmpil_at(dst_addr, exec->ds->recvs.displs[i], &exec_mpi->zmpil_types[dst_exec_id]))[x]);
#endif
  }

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}

#endif /* MAKE_WITH_ALLTOALLW */


dsint_t ds_exec_mpi_add_address(ds_exec_t *exec, void *addr) /* ds_func ds_exec_mpi_add_address */
{
  Z_TRACE_IF(DS_TRACE_IF, "addr = %p", addr);

  return ds_exec_add_address(exec, addr);
}


static void ds_exec_mpi_move(ds_exec_t *exec, dsint_t exec_id, dsint_t src_buf_id, dsint_t src_displ, dsint_t dst_buf_id, dsint_t dst_displ, dsint_t count)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  void *src_addr, *dst_addr;


  src_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[src_buf_id].addr_id], src_displ, &exec_mpi->zmpil_types[exec_id]);
  dst_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[dst_buf_id].addr_id], dst_displ, &exec_mpi->zmpil_types[exec_id]);

#ifdef PRINT_CONTENT
  dsint_t x;
  for (x = 0; x < count; ++x)
    Z_TRACE_IF(DS_TRACE_IF, "move [%" dsint_fmt "]: %lld\n", x, ((long long *) src_addr)[x]);
#endif

  if (src_buf_id != dst_buf_id || src_displ + count < dst_displ || dst_displ + count < src_displ)
    zmpil_memcpy_conv(dst_addr, src_addr, count, &exec_mpi->zmpil_types[exec_id], &exec_mpi->zmpil_types[exec_id]);
  else
    zmpil_memmove_conv(dst_addr, src_addr, count, &exec_mpi->zmpil_types[exec_id], &exec_mpi->zmpil_types[exec_id]);
}


#ifdef DASH_SYMMETRIC

static void ds_exec_mpi_sendrecv_replace(ds_exec_t *exec, int proc, dsint_t exec_id, dsint_t buf_id, dspint_t displ, dspint_t count)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);


  void *addr;
  MPI_Status status;

  Z_TRACE_IF(DS_TRACE_IF, "proc: %d, buf_id: %" dsint_fmt ", displ: %" dspint_fmt ", count: %" dspint_fmt, proc, buf_id, displ, count);

  addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[buf_id].addr_id], displ, &exec_mpi->zmpil_types[exec_id]);

  MPI_Sendrecv_replace(addr, count, exec_mpi->mpi_types[exec_id],
    proc, DS_EXEC_MPI_SENDRECV_REPLACE_TAG, proc, DS_EXEC_MPI_SENDRECV_REPLACE_TAG, exec->ds->comm, &status);
}

#ifdef DASH_SYMMETRIC_AUX

/*#define RECV_REQ_ONLY*/

#if defined(DASH_SYMMETRIC_AUX_IMMEDIATELY) || defined(DASH_EXEC_MPI_REQUESTS_LIMIT)
# undef RECV_REQ_ONLY
#endif

#ifdef RECV_REQ_ONLY
# define RECV_REQ_ONLY_IF_ELSE(_if_, _else_)  _if_
#else
# define RECV_REQ_ONLY_IF_ELSE(_if_, _else_)  _else_
#endif

static void ds_exec_mpi_sendrecv_aux_setup(ds_exec_t *exec, dsint_t max_nsyms)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);


  Z_ASSERT(max_nsyms * 2 <= exec_mpi->nmax);

  exec_mpi->sym_aux_nreqs = 0;
}


static dsint_t ds_exec_mpi_sendrecv_aux(ds_exec_t *exec, int proc, dsint_t exec_id, dsint_t buf_id, dspint_t displ, dspint_t count, dsint_t aux_exec_id, dsint_t aux_buf_id, dspint_t aux_displ)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  void *buf_addr, *aux_addr;
#ifdef RECV_REQ_ONLY
  MPI_Request req;
#endif


  Z_TRACE_IF(DS_TRACE_IF, "proc: %d, buf_id: %" dsint_fmt ", displ: %" dspint_fmt ", count: %" dspint_fmt ", aux_buf_id: %" dsint_fmt ", aux_displ: %" dspint_fmt, proc, buf_id, displ, count, aux_buf_id, aux_displ);

  if (exec_mpi->sym_aux_nreqs + RECV_REQ_ONLY_IF_ELSE(1, 2) > exec_mpi->nmax
#ifdef DASH_EXEC_MPI_REQUESTS_LIMIT
    || exec_mpi->sym_aux_nreqs + RECV_REQ_ONLY_IF_ELSE(1, 2) > DASH_EXEC_MPI_REQUESTS_LIMIT
#endif
    ) return 0;

  buf_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[buf_id].addr_id], displ, &exec_mpi->zmpil_types[exec_id]);
  aux_addr = zmpil_at(exec->addrs[exec->ds->sched->bufs[aux_buf_id].addr_id], aux_displ, &exec_mpi->zmpil_types[aux_exec_id]);

  zmpil_memcpy_conv(aux_addr, buf_addr, count, &exec_mpi->zmpil_types[aux_exec_id], &exec_mpi->zmpil_types[exec_id]);

  MPI_Irecv(buf_addr, count, exec_mpi->mpi_types[exec_id], proc, DS_EXEC_MPI_SENDRECV_AUX_TAG, exec->ds->comm, &exec_mpi->reqs[exec_mpi->sym_aux_nreqs]);
  ++exec_mpi->sym_aux_nreqs;

#ifdef RECV_REQ_ONLY
  MPI_Isend(aux_addr, count, exec_mpi->mpi_types[aux_exec_id], proc, DS_EXEC_MPI_SENDRECV_AUX_TAG, exec->ds->comm, &req);
  MPI_Request_free(&req);
#else
  MPI_Isend(aux_addr, count, exec_mpi->mpi_types[aux_exec_id], proc, DS_EXEC_MPI_SENDRECV_AUX_TAG, exec->ds->comm, &exec_mpi->reqs[exec_mpi->sym_aux_nreqs]);
  ++exec_mpi->sym_aux_nreqs;
#endif

  return 1;
}


static void ds_exec_mpi_sendrecv_aux_intermediate(ds_exec_t *exec)
{
  DEFINE_EXEC_MPI(exec, exec_mpi);


  Z_TRACE_IF(DS_TRACE_IF, "wait for %" dsint_fmt " request(s)", exec_mpi->sym_aux_nreqs);

  MPI_Waitall(exec_mpi->sym_aux_nreqs, exec_mpi->reqs, exec_mpi->stats);

  exec_mpi->sym_aux_nreqs = 0;
}


static void ds_exec_mpi_sendrecv_aux_finish(ds_exec_t *exec)
{
  ds_exec_mpi_sendrecv_aux_intermediate(exec);

#ifdef RECV_REQ_ONLY
  MPI_Barrier(exec->ds->comm);
#endif
}

#undef RECV_REQ_ONLY
#undef RECV_REQ_ONLY_IF_ELSE

#endif /* DASH_SYMMETRIC_AUX */

#endif /* DASH_SYMMETRIC */


dsint_t ds_exec_mpi_add_type(ds_exec_t *exec, MPI_Datatype type) /* ds_func ds_exec_mpi_add_type */
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t exec_id = -1;


  Z_TRACE_IF(DS_TRACE_IF, "START");

  if (exec_mpi->ntypes < DASH_MAX_NBUFFERS)
  {
    exec_id = exec_mpi->ntypes;
    ++exec_mpi->ntypes;

    exec_mpi->mpi_types[exec_id] = type;
    zmpil_create(&exec_mpi->zmpil_types[exec_id], type, 1);
  }

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return exec_id;
}


dsint_t ds_exec_mpi_sizefor(ds_exec_t *exec, dsint_t exec_id, dsint_t size) /* ds_func ds_exec_mpi_sizefor */
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t sizefor;


  sizefor = zmpil_sizefor(size, &exec_mpi->zmpil_types[exec_id]);

  return sizefor;
}


dsint_t ds_exec_mpi_extent(ds_exec_t *exec, dsint_t exec_id) /* ds_func ds_exec_mpi_extent */
{
  DEFINE_EXEC_MPI(exec, exec_mpi);
  dsint_t extent;


  extent = zmpil_extent(&exec_mpi->zmpil_types[exec_id]);

  return extent;
}


#undef DS_TRACE_IF
