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


#include "dash_sched_a2av_sym.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif


#ifdef DASH_SYMMETRIC


static dsint_t ds_sched_a2av_sym_max_n(ds_sched_t *sched, dsint_t *max_nsends, dsint_t *max_n, dsint_t *max_nlocals, dsint_t *max_nsyms);
/*static dsint_t ds_sched_a2av_sym_pre_run(ds_sched_t *sched);
static dsint_t ds_sched_a2av_sym_post_run(ds_sched_t *sched);*/
static dsint_t ds_sched_a2av_sym_finished(ds_sched_t *sched);
static dsint_t ds_sched_a2av_sym_pre(ds_sched_t *sched);
/*static dsint_t ds_sched_a2av_sym_post(ds_sched_t *sched);*/


dsint_t ds_sched_a2av_sym_create(ds_sched_t *sched) /* ds_func ds_sched_a2av_sym_create */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  ds_sched_create(sched);

  sched->max_n = ds_sched_a2av_sym_max_n;
  sched->finished = ds_sched_a2av_sym_finished;
  sched->pre = ds_sched_a2av_sym_pre;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_sched_a2av_sym_destroy(ds_sched_t *sched) /* ds_func ds_sched_a2av_sym_destroy */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  ds_sched_destroy(sched);

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


static dsint_t ds_sched_a2av_sym_max_n(ds_sched_t *sched, dsint_t *max_nsends, dsint_t *max_nrecvs, dsint_t *max_nlocals, dsint_t *max_nsyms)
{
#ifdef DASH_SYMMETRIC_AUX
  *max_nsends = 0;
  *max_nrecvs = 2 * sched->recv.n;
#else
  *max_nsends = 0;
  *max_nrecvs = 0;
#endif
  *max_nlocals = 0;

  *max_nsyms = sched->recv.n;

  return 0;
}


static dsint_t ds_sched_a2av_sym_finished(ds_sched_t *sched)
{
  if (sched->cxt) return 1;

  sched->cxt = (void *) 1;

  return 0;
}


static dsint_t ds_sched_a2av_sym_pre(ds_sched_t *sched)
{
  dsint_t i;


  sched->ds->syms.n = sched->recv.n;
  sched->ds->syms.execute = 0;
  sched->ds->syms.exec_id = sched->send.exec_id;

  for (i = 0; i < sched->recv.n; ++i)
  {
    sched->ds->syms.buf_ids[i] = 0;
    sched->ds->syms.counts[i] = sched->recv.counts[i];
    sched->ds->syms.displs[i] = sched->recv.displs[i];
    sched->ds->syms.proc_ids[i] = i;

    if (sched->recv.counts[i]) sched->ds->syms.execute = 1;
  }

  return 0;
}

#endif /* DASH_SYMMETRIC */


#undef DS_TRACE_IF
