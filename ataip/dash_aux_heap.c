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
#include "local_generic_heap.h"


#ifndef DS_TRACE_IF
# define DS_TRACE_IF  (z_mpi_rank == -1)
#endif

#define MAP_PROC_TO_BLOCK(_p_, _n_)  (_p_ % _n_)


typedef struct _aux_heap_t
{
  lgh_t lgh;

  dspint_t nblocks;
  struct {
    lgh_segment_t *seg;
    dsint_t begin, end;

  } *blocks;

} aux_heap_t;


static dsint_t ds_aux_heap_acquire(ds_sched_a2av_aux_t *aux, dsint_t count, dspint_t proc_id);
static dsint_t ds_aux_heap_get_count(ds_sched_a2av_aux_t *aux, dspint_t proc_id);
static dsint_t ds_aux_heap_get_displ(ds_sched_a2av_aux_t *aux, dspint_t proc_id);
static void ds_aux_heap_vacate(ds_sched_a2av_aux_t *aux, void *schedptr);
static void ds_aux_heap_accept_recv(ds_sched_a2av_aux_t *aux, dspint_t proc_id, dsint_t count);


void ds_aux_heap_create(ds_sched_a2av_aux_t *aux, dsint_t size, dsint_t nblocks) /* ds_func ds_aux_heap_create */
{
  aux_heap_t *d;
  dsint_t i;


  ds_aux_create(aux, size);

  aux->acquire = ds_aux_heap_acquire;

  aux->get_count = ds_aux_heap_get_count;
  aux->get_displ = ds_aux_heap_get_displ;

  aux->vacate = ds_aux_heap_vacate;

  aux->accept_recv = ds_aux_heap_accept_recv;

  d = z_alloc(1, sizeof(aux_heap_t));

  aux->data = d;

  lgh_create(&d->lgh, size);

  d->nblocks = nblocks;

  Z_TRACE_IF(DS_TRACE_IF, "nblocks: %" dsint_fmt, nblocks);

  d->blocks = z_alloc(d->nblocks, sizeof(*d->blocks));

  for (i = 0; i < d->nblocks; ++i)
  {
    d->blocks[i].seg = NULL;
    d->blocks[i].begin = d->blocks[i].end = -1;
  }
}


void ds_aux_heap_destroy(ds_sched_a2av_aux_t *aux) /* ds_func ds_aux_heap_destroy */
{
  aux_heap_t *d = (aux_heap_t *) aux->data;

  lgh_destroy(&d->lgh);

  z_free(d->blocks);

  z_free(aux->data);

  ds_aux_destroy(aux);
}


static dsint_t ds_aux_heap_acquire(ds_sched_a2av_aux_t *aux, dsint_t count, dspint_t proc_id)
{
  aux_heap_t *d = (aux_heap_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  if (d->blocks[i].begin < d->blocks[i].end) return 0;

  d->blocks[i].seg = lgh_alloc_minmax(&d->lgh, 1, count);

  if (d->blocks[i].seg == NULL) return 0;

  d->blocks[i].begin = d->blocks[i].seg->offset;
  d->blocks[i].end = d->blocks[i].seg->offset + d->blocks[i].seg->size;

  Z_TRACE_IF(DS_TRACE_IF, "acquired: %" dsint_fmt " (of %" dsint_fmt ") from %" dspint_fmt, d->blocks[i].end - d->blocks[i].begin, count, proc_id);

  return d->blocks[i].end - d->blocks[i].begin;
}


static dsint_t ds_aux_heap_get_count(ds_sched_a2av_aux_t *aux, dspint_t proc_id)
{
  aux_heap_t *d = (aux_heap_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  Z_TRACE_IF(DS_TRACE_IF, "count: %" dsint_fmt, d->blocks[i].end - d->blocks[i].begin);

  return d->blocks[i].end - d->blocks[i].begin;
}


static dsint_t ds_aux_heap_get_displ(ds_sched_a2av_aux_t *aux, dspint_t proc_id)
{
  aux_heap_t *d = (aux_heap_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  Z_TRACE_IF(DS_TRACE_IF, "displ: %" dsint_fmt, d->blocks[i].begin);

  return d->blocks[i].begin;
}


static void ds_aux_heap_vacate(ds_sched_a2av_aux_t *aux, void *schedptr)
{
  aux_heap_t *d = (aux_heap_t *) aux->data;
  dsint_t i, count;


  for (i = 0; i < d->nblocks; ++i)
  if (d->blocks[i].begin < d->blocks[i].end)
  {
    count = aux->vacate_aux(schedptr, i, d->blocks[i].end - d->blocks[i].begin, d->blocks[i].begin);

    d->blocks[i].begin += count;

    if (d->blocks[i].begin >= d->blocks[i].end)
    {
      lgh_free(&d->lgh, d->blocks[i].seg);
      d->blocks[i].seg = NULL;
    }

    Z_TRACE_IF(DS_TRACE_IF, "moved %" dsint_fmt " and %" dsint_fmt " are left%s", count, d->blocks[i].end - d->blocks[i].begin, (d->blocks[i].end >= d->blocks[i].begin)?" -> empty":"");
  }
}


static void ds_aux_heap_accept_recv(ds_sched_a2av_aux_t *aux, dspint_t proc_id, dsint_t count)
{
  aux_heap_t *d = (aux_heap_t *) aux->data;
  dsint_t i;


  i = MAP_PROC_TO_BLOCK(proc_id, d->nblocks);

  d->blocks[i].end = d->blocks[i].begin + count;

  if (count <= 0)
  {
    lgh_free(&d->lgh, d->blocks[i].seg);
    d->blocks[i].seg = NULL;
  }
}


#undef DS_TRACE_IF

#undef MAP_PROC_TO_BLOCK
