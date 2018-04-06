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


#include "dash_sched.h"
#include "dash_core.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif


dsint_t ds_sched_create(ds_sched_t *sched) /* ds_func ds_sched_create */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  sched->round = 0;

  sched->nbufs = 0;

  sched->max_n = NULL;
  sched->pre_run = NULL;
  sched->post_run = NULL;
  sched->finished = NULL;
  sched->pre = NULL;
  sched->post = NULL;

  sched->cxt = NULL;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_sched_destroy(ds_sched_t *sched) /* ds_func ds_sched_destroy */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return 0;
}


dsint_t ds_sched_add_buffer(ds_sched_t *sched, dsint_t addr_id) /* ds_func ds_sched_add_buffer */
{
  dsint_t buf_id = -1;

  if (sched->nbufs < DASH_MAX_NBUFFERS)
  {
    buf_id = sched->nbufs;
    ++sched->nbufs;

    sched->bufs[buf_id].addr_id = addr_id;
  }

  return buf_id;
}


dsint_t ds_sched_set_send(ds_sched_t *sched, dsint_t buf_id, dsint_t n, dspint_t *scounts, dspint_t *sdispls, dspint_t *sranks, dsint_t exec_id) /* ds_func ds_sched_set_send */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  sched->send.buf_id = buf_id;
  sched->send.exec_id = exec_id;

  sched->send.n = n;
  sched->send.counts = scounts;
  sched->send.displs = sdispls;
  sched->send.ranks = sranks;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return buf_id;
}


dsint_t ds_sched_set_recv(ds_sched_t *sched, dsint_t buf_id, dsint_t n, dspint_t *rcounts, dspint_t *rdispls, dspint_t *rranks, dsint_t exec_id) /* ds_func ds_sched_set_recv */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  sched->recv.buf_id = buf_id;
  sched->recv.exec_id = exec_id;

  sched->recv.n = n;
  sched->recv.counts = rcounts;
  sched->recv.displs = rdispls;
  sched->recv.ranks = rranks;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return buf_id;
}


dsint_t ds_sched_set_aux(ds_sched_t *sched, dsint_t buf_id, dsint_t buf_size) /* ds_func ds_sched_set_aux */
{
  Z_TRACE_IF(DS_TRACE_IF, "START");

  sched->aux.buf_id = buf_id;
  sched->aux.buf_size = buf_size;

  Z_TRACE_IF(DS_TRACE_IF, "END");

  return buf_id;
}


#undef DS_TRACE_IF
