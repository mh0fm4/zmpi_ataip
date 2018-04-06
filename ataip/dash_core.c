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


#include "dash_core.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif


dsint_t ds_core_run_sync = 0; /* ds_var ds_core_run_sync */

double ds_times[DS_TIMES_RUN_NTIMES];  /* ds_var ds_times */


dsint_t ds_create(ds_t *ds, ds_sched_t *sched, ds_exec_t *exec) /* ds_func ds_create */
{
  dsint_t max_nsends, max_nrecvs, max_nlocals, max_nsyms;

  Z_TRACE_IF(DS_TRACE_IF, "START");

  ds->sched = sched; sched->ds = ds;
  ds->exec = exec; exec->ds = ds;

  sched->max_n(sched, &max_nsends, &max_nrecvs, &max_nlocals, &max_nsyms);

  ds->sends.nmax = max_nsends;
  ds->sends.n = 0;
  ds->sends.execute = 0;
  ds->sends.buf_ids = z_alloc(max_nsends, sizeof(dsint_t));
  ds->sends.counts   = z_alloc(max_nsends * 3, sizeof(dspint_t));
  ds->sends.displs   = ds->sends.counts + 1 * max_nsends;
  ds->sends.proc_ids = ds->sends.counts + 2 * max_nsends;
  ds->sends.ranks = NULL;

  ds->recvs.nmax = max_nrecvs;
  ds->recvs.n = 0;
  ds->recvs.execute = 0;
  ds->recvs.buf_ids = z_alloc(max_nrecvs, sizeof(dsint_t));
  ds->recvs.counts   = z_alloc(max_nrecvs * 3, sizeof(dspint_t));
  ds->recvs.displs   = ds->recvs.counts + 1 * max_nrecvs;
  ds->recvs.proc_ids = ds->recvs.counts + 2 * max_nrecvs;
  ds->recvs.ranks = NULL;

  ds->locals.nmax = max_nlocals;
  ds->locals.n = 0;
  ds->locals.execute = 0;
  ds->locals.src_buf_ids  = z_alloc(max_nlocals * 4, sizeof(dsint_t));
  ds->locals.dst_buf_ids  = ds->locals.src_buf_ids + 1 * max_nlocals;
  ds->locals.src_exec_ids = ds->locals.src_buf_ids + 2 * max_nlocals;
  ds->locals.dst_exec_ids = ds->locals.src_buf_ids + 3 * max_nlocals;
  ds->locals.src_counts = z_alloc(max_nlocals * 4, sizeof(dspint_t));
  ds->locals.src_displs = ds->locals.src_counts + 1 * max_nlocals;
  ds->locals.dst_counts = ds->locals.src_counts + 2 * max_nlocals;
  ds->locals.dst_displs = ds->locals.src_counts + 3 * max_nlocals;

#ifdef DASH_SYMMETRIC
  ds->syms.nmax = max_nsyms;
  ds->syms.n = 0;
  ds->syms.execute = 0;
  ds->syms.buf_ids = z_alloc(max_nsyms, sizeof(dsint_t));
  ds->syms.counts   = z_alloc(max_nsyms * 3, sizeof(dspint_t));
  ds->syms.displs   = ds->syms.counts + 1 * max_nsyms;
  ds->syms.proc_ids = ds->syms.counts + 2 * max_nsyms;
  ds->syms.ranks = NULL;
#endif

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_destroy(ds_t *ds) /* ds_func ds_destroy */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  ds->sched = NULL;
  ds->exec = NULL;

  z_free(ds->sends.buf_ids);
  ds->sends.buf_ids = NULL;
  z_free(ds->sends.counts);
  ds->sends.counts = ds->sends.displs = ds->sends.proc_ids = NULL;
  ds->sends.ranks = NULL;

  z_free(ds->recvs.buf_ids);
  ds->recvs.buf_ids = NULL;
  z_free(ds->recvs.counts);
  ds->recvs.counts = ds->recvs.displs = ds->recvs.proc_ids = NULL;
  ds->recvs.ranks = NULL;

  z_free(ds->locals.src_buf_ids);
  ds->locals.src_buf_ids = ds->locals.dst_buf_ids = ds->locals.src_exec_ids = ds->locals.dst_exec_ids = NULL;
  z_free(ds->locals.src_counts);
  ds->locals.src_counts = ds->locals.src_displs = ds->locals.dst_counts = ds->locals.dst_displs = NULL;

#ifdef DASH_SYMMETRIC
  z_free(ds->syms.buf_ids);
  ds->syms.buf_ids = NULL;
  z_free(ds->syms.counts);
  ds->syms.counts = ds->syms.displs = ds->syms.proc_ids = NULL;
  ds->syms.ranks = NULL;
#endif

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


static void ds_print(ds_t *ds)
{
  Z_TRACE_DECLARE(dsint_t i;)


  Z_TRACE_IF(DS_TRACE_IF, "sends: execute: %" dsint_fmt ", n: %" dsint_fmt " / %" dsint_fmt, ds->sends.execute, ds->sends.n, ds->sends.nmax);

  if (ds->sends.execute)
  {
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->sends.n, " %" dspint_fmt, ds->sends.counts[i], "send counts:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->sends.n, " %" dspint_fmt, ds->sends.displs[i], "send displs:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->sends.n, " %" dspint_fmt, ds->sends.proc_ids[i], "send proc_ids:");
  }

  Z_TRACE_IF(DS_TRACE_IF, "receives: execute: %" dsint_fmt ", n: %" dsint_fmt " / %" dsint_fmt, ds->recvs.execute, ds->recvs.n, ds->recvs.nmax);

  if (ds->recvs.execute)
  {
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->recvs.n, " %" dspint_fmt, ds->recvs.counts[i], "recv counts:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->recvs.n, " %" dspint_fmt, ds->recvs.displs[i], "recv displs:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->recvs.n, " %" dspint_fmt, ds->recvs.proc_ids[i], "recv proc_ids:");
  }

  Z_TRACE_IF(DS_TRACE_IF, "locals: execute: %" dsint_fmt ", n: %" dsint_fmt " / %" dsint_fmt, ds->locals.execute, ds->locals.n, ds->locals.nmax);

  if (ds->locals.execute)
  {
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->locals.n, " %" dspint_fmt, ds->locals.src_counts[i], "local src counts:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->locals.n, " %" dspint_fmt, ds->locals.src_displs[i], "local src displs:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->locals.n, " %" dspint_fmt, ds->locals.dst_counts[i], "local dst counts:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->locals.n, " %" dspint_fmt, ds->locals.dst_displs[i], "local dst displs:");
  }

#ifdef DASH_SYMMETRIC
  Z_TRACE_IF(DS_TRACE_IF, "syms: execute: %" dsint_fmt ", n: %" dsint_fmt " / %" dsint_fmt, ds->syms.execute, ds->syms.n, ds->syms.nmax);

  if (ds->syms.execute)
  {
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->syms.nmax, " %" dspint_fmt, ds->syms.counts[i], "sym counts:");
    Z_TRACE_ARRAY_IF(DS_TRACE_IF, i, ds->syms.nmax, " %" dspint_fmt, ds->syms.displs[i], "sym displs:");
  }
#endif
}


dsint_t ds_run(ds_t *ds) /* ds_func ds_run */
{
#ifdef DASH_TIMING
  double t;
#endif


  Z_TRACE_IF(DS_TRACE_IF, "START");

  if (ds_core_run_sync) MPI_Barrier(ds->comm);
  DS_TIMING_CMD(ds_times[DS_TIMES_RUN] = z_time_get_s(););
  DS_TIMING_CMD(ds_times[DS_TIMES_RUN_PRE] = z_time_get_s(););

  if (ds->sched->pre_run) ds->sched->pre_run(ds->sched);
  ds->exec->pre_run(ds->exec);

  if (ds_core_run_sync) MPI_Barrier(ds->comm);
  DS_TIMING_CMD(ds_times[DS_TIMES_RUN_PRE] = z_time_get_s() - ds_times[DS_TIMES_RUN_PRE];);
  DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE] = z_time_get_s(););

  DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE_PRE] = ds_times[DS_TIMES_RUN_WHILE_MAKE] = ds_times[DS_TIMES_RUN_WHILE_POST] = ds_times[DS_TIMES_RUN_WHILE_FINISHED] = 0;);

  DS_TIMING_CMD(t = z_time_get_s(););

  while (!ds->sched->finished(ds->sched))
  {
    if (ds_core_run_sync) MPI_Barrier(ds->comm);
    DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE_FINISHED] += z_time_get_s() - t; t = z_time_get_s(););

    if (ds->sched->pre) ds->sched->pre(ds->sched);

    if (ds_core_run_sync) MPI_Barrier(ds->comm);
    DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE_PRE] += z_time_get_s() - t; t = z_time_get_s(););

    ds_print(ds);

    ds_exec_make(ds->exec);

    if (ds_core_run_sync) MPI_Barrier(ds->comm);
    DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE_MAKE] += z_time_get_s() - t; t = z_time_get_s(););

    if (ds->sched->post) ds->sched->post(ds->sched);

    if (ds_core_run_sync) MPI_Barrier(ds->comm);
    DS_TIMING_CMD(ds_times[DS_TIMES_RUN_WHILE_POST] += z_time_get_s() - t; t = z_time_get_s(););
  }

  DS_TIMING_CMD(
    ds_times[DS_TIMES_RUN_WHILE_FINISHED] += z_time_get_s() - t;

    ds_times[DS_TIMES_RUN_WHILE_ROUNDS] = ds->sched->round;

    ds_times[DS_TIMES_RUN_WHILE] = z_time_get_s() - ds_times[DS_TIMES_RUN_WHILE];
    ds_times[DS_TIMES_RUN_POST] = z_time_get_s();
  );

  if (ds->sched->post_run) ds->sched->post_run(ds->sched);
  ds->exec->post_run(ds->exec);

  if (ds_core_run_sync) MPI_Barrier(ds->comm);
  DS_TIMING_CMD(
    ds_times[DS_TIMES_RUN_POST] = z_time_get_s() - ds_times[DS_TIMES_RUN_POST];
    ds_times[DS_TIMES_RUN] = z_time_get_s() - ds_times[DS_TIMES_RUN];
  );

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


#undef DS_TRACE_IF
