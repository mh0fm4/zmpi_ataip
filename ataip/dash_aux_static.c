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

#define MAP_PROC_TO_BLOCK(_p_, _n_)  (_p_ % _n_)


typedef struct _aux_static_t
{
  dspint_t nblocks;
  struct {
    dspint_t proc_id;
    dsint_t base, size, new;
    dsint_t begin, end;

  } *blocks;

} aux_static_t;


static dsint_t ds_aux_static_acquire(ds_sched_a2av_aux_t *aux, dsint_t count, dspint_t proc_id);
static dsint_t ds_aux_static_get_count(ds_sched_a2av_aux_t *aux, dspint_t proc_id);
static dsint_t ds_aux_static_get_displ(ds_sched_a2av_aux_t *aux, dspint_t proc_id);
static void ds_aux_static_vacate(ds_sched_a2av_aux_t *aux, void *schedptr);
static void ds_aux_static_accept_recv(ds_sched_a2av_aux_t *aux, dspint_t proc_id, dsint_t count);


void ds_aux_static_create(ds_sched_a2av_aux_t *aux, dsint_t size, dsint_t nblocks) /* ds_func ds_aux_static_create */
{
  aux_static_t *d;
  dsint_t i, base;


  ds_aux_create(aux, size);

  aux->acquire = ds_aux_static_acquire;

  aux->get_count = ds_aux_static_get_count;
  aux->get_displ = ds_aux_static_get_displ;

  aux->vacate = ds_aux_static_vacate;

  aux->accept_recv = ds_aux_static_accept_recv;

  d = z_alloc(1, sizeof(aux_static_t));

  aux->data = d;

  d->nblocks = z_minmax(1, nblocks, size);

  Z_TRACE_IF(DS_TRACE_IF, "nblocks: %" dsint_fmt, nblocks);

  d->blocks = z_alloc(d->nblocks, sizeof(*d->blocks));

  base = 0;
  for (i = 0; i < d->nblocks; ++i)
  {
    d->blocks[i].proc_id = -1;
    d->blocks[i].new = 0;
    d->blocks[i].base = base;
    d->blocks[i].size = (dsint_t) z_round((double) (size - base) / (double) (d->nblocks - i));
    d->blocks[i].begin = d->blocks[i].end = base;

    base += d->blocks[i].size;
  }
}


void ds_aux_static_destroy(ds_sched_a2av_aux_t *aux) /* ds_func ds_aux_static_destroy */
{
  aux_static_t *d = (aux_static_t *) aux->data;


  z_free(d->blocks);

  z_free(aux->data);

  ds_aux_destroy(aux);
}


static dsint_t ds_aux_static_acquire(ds_sched_a2av_aux_t *aux, dsint_t count, dspint_t proc_id)
{
  aux_static_t *d = (aux_static_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  if (d->blocks[i].begin < d->blocks[i].end) return 0;

  d->blocks[i].proc_id = proc_id;
  d->blocks[i].new = 1;
  d->blocks[i].begin = d->blocks[i].base;
  d->blocks[i].end = d->blocks[i].begin + z_min(d->blocks[i].size, count);

  Z_TRACE_IF(DS_TRACE_IF, "acquired: %" dsint_fmt " (of %" dsint_fmt ") from %" dspint_fmt, d->blocks[i].end - d->blocks[i].begin, count, d->blocks[i].proc_id);

  return d->blocks[i].end - d->blocks[i].begin;
}


static dsint_t ds_aux_static_get_count(ds_sched_a2av_aux_t *aux, dspint_t proc_id)
{
  aux_static_t *d = (aux_static_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  if (d->blocks[i].proc_id != proc_id) return 0;

  Z_TRACE_IF(DS_TRACE_IF, "count: %" dsint_fmt, d->blocks[i].end - d->blocks[i].begin);

  return d->blocks[i].end - d->blocks[i].begin;
}


static dsint_t ds_aux_static_get_displ(ds_sched_a2av_aux_t *aux, dspint_t proc_id)
{
  aux_static_t *d = (aux_static_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  Z_ASSERT(d->blocks[i].new);
  Z_ASSERT(d->blocks[i].proc_id == proc_id);

  Z_TRACE_IF(DS_TRACE_IF, "displ: %" dsint_fmt, d->blocks[i].begin);

  return d->blocks[i].begin;
}


static void ds_aux_static_vacate(ds_sched_a2av_aux_t *aux, void *schedptr)
{
  aux_static_t *d = (aux_static_t *) aux->data;
  dsint_t i, count;


  for (i = 0; i < d->nblocks; ++i)
  if (d->blocks[i].begin < d->blocks[i].end)
  {
    count = aux->vacate_aux(schedptr, d->blocks[i].proc_id, d->blocks[i].end - d->blocks[i].begin, d->blocks[i].begin);

    d->blocks[i].begin += count;

    Z_TRACE_IF(DS_TRACE_IF, "moved %" dsint_fmt " and %" dsint_fmt " are left%s", count, d->blocks[i].end - d->blocks[i].begin, (d->blocks[i].end >= d->blocks[i].begin)?" -> empty":"");
  }
}


static void ds_aux_static_accept_recv(ds_sched_a2av_aux_t *aux, dspint_t proc_id, dsint_t count)
{
  aux_static_t *d = (aux_static_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  Z_ASSERT(d->blocks[i].new);
  Z_ASSERT(d->blocks[i].proc_id == proc_id);


  d->blocks[i].new = 0;
  d->blocks[i].end = d->blocks[i].begin + count;
}


#undef DS_TRACE_IF

#undef MAP_PROC_TO_BLOCK
