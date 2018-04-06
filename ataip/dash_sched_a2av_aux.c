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


#include "dash_sched_a2av_aux.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif


void ds_aux_create(ds_sched_a2av_aux_t *aux, dsint_t size) /* ds_func ds_aux_create */
{
  aux->size = size;

  Z_TRACE_IF(DS_TRACE_IF, "size: %" dsint_fmt, size);

  aux->pre_acquire = NULL;
  aux->acquire = NULL;
  aux->post_acquire = NULL;

  aux->get_count = NULL;
  aux->get_displ = NULL;

  aux->vacate = NULL;

  aux->accept_recv = NULL;
}


void ds_aux_destroy(ds_sched_a2av_aux_t *aux) /* ds_func ds_aux_destroy */
{
}


void ds_aux_pre_acquire(ds_sched_a2av_aux_t *aux) /* ds_func ds_aux_pre_acquire */
{
  if (aux->pre_acquire) aux->pre_acquire(aux);
}


dsint_t ds_aux_acquire(ds_sched_a2av_aux_t *aux, dsint_t count, dspint_t proc_id) /* ds_func ds_aux_acquire */
{
  return aux->acquire(aux, count, proc_id);
}


void ds_aux_post_acquire(ds_sched_a2av_aux_t *aux) /* ds_func ds_aux_post_acquire */
{
  if (aux->post_acquire) aux->post_acquire(aux);
}


dsint_t ds_aux_get_count(ds_sched_a2av_aux_t *aux, dspint_t proc_id) /* ds_func ds_aux_get_count */
{
  return aux->get_count(aux, proc_id);
}


dsint_t ds_aux_get_displ(ds_sched_a2av_aux_t *aux, dspint_t proc_id) /* ds_func ds_aux_get_displ */
{
  return aux->get_displ(aux, proc_id);
}


void ds_aux_vacate(ds_sched_a2av_aux_t *aux, void *schedptr) /* ds_func ds_aux_vacate */
{
  aux->vacate(aux, schedptr);
}


void ds_aux_accept_recv(ds_sched_a2av_aux_t *aux, dspint_t proc_id, dsint_t count) /* ds_func ds_aux_accept_recv */
{
  aux->accept_recv(aux, proc_id, count);
}


#undef DS_TRACE_IF
