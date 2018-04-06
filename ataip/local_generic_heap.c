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


#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits.h>

#include "z_pack.h"
#include "local_generic_heap.h"


#define LGH_TRACE_IF  (z_mpi_rank == -1)


void lgh_create(lgh_t *lgh, lghint_t size) /* lgh_func lgh_create */
{
  Z_TRACE_IF(LGH_TRACE_IF, "size: %" lghint_fmt, size);

  lgh->nallocs = 0;

  lgh->segments = z_alloc(1, sizeof(lgh_segment_t));
  lgh->segments->next = NULL;
  lgh->segments->offset = 0;
  lgh->segments->size = size;
}


void lgh_destroy(lgh_t *lgh) /* lgh_func lgh_destroy */
{
  lgh_segment_t *seg;


  Z_ASSERT(lgh->nallocs == 0);
  Z_ASSERT(lgh->segments != NULL);
  Z_ASSERT(lgh->segments != NULL && lgh->segments->next == NULL);

  Z_TRACE_IF(LGH_TRACE_IF, "nallocs: %" lghint_fmt, lgh->nallocs);

  lgh->nallocs = 0;
  while (lgh->segments)
  {
    seg = lgh->segments->next;
    z_free(lgh->segments);
    lgh->segments = seg;
  }
}


lgh_segment_t *lgh_alloc(lgh_t *lgh, lghint_t size) /* lgh_func lgh_alloc */
{
  return lgh_alloc_minmax(lgh, size, size);
}


lgh_segment_t *lgh_alloc_minmax(lgh_t *lgh, lghint_t min, lghint_t max) /* lgh_func lgh_alloc_minmax */
{
  lgh_segment_t *curr, *prev, *best, *best_prev, *seg;
  lghint_t best_size;


  if (min > max) return NULL;

  Z_TRACE_IF(LGH_TRACE_IF, "searching for minmax: %" lghint_fmt "-%" lghint_fmt, min, max);

  best = best_prev = NULL;
  best_size = min - 1;

  prev = NULL;
  curr = lgh->segments;
  while (curr != NULL && best_size != max)
  {
    if (best_size < max)
    {
      if (curr->size > best_size)
      {
        best = curr;
        best_prev = prev;
        best_size = curr->size;
      }

    } else
    {
      if (max <= curr->size && curr->size < best_size)
      {
        best = curr;
        best_prev = prev;
        best_size = curr->size;
      }
    }

    prev = curr;
    curr = curr->next;

/*    if (!local_heap_alloc_search) continue;*/
  }

  max = z_min(max, best_size);
  curr = best;
  prev = best_prev;

  if (curr == NULL) return NULL;

  if (curr->size <= max)
  {
    seg = curr;

    if (prev == NULL) lgh->segments = curr->next;
    else prev->next = curr->next;

  } else
  {
    seg = z_alloc(1, sizeof(lgh_segment_t));
    seg->next = NULL;
    seg->offset = curr->offset;
    seg->size = max;

    curr->offset += max;
    curr->size -= max;
  }

  ++lgh->nallocs;

  Z_TRACE_IF(LGH_TRACE_IF, "allocated segment %p: [%" lghint_fmt ", %" lghint_fmt "]", seg, seg->offset, seg->size);

  Z_TRACE_IF(LGH_TRACE_IF, "nallocs OUT: %" lghint_fmt, lgh->nallocs);

  return seg;
}


void lgh_free(lgh_t *lgh, lgh_segment_t *seg) /* lgh_func lgh_free */
{
  lgh_segment_t *prev, *curr;


  if (seg == NULL) return;

  Z_TRACE_IF(LGH_TRACE_IF, "nallocs IN: %" lghint_fmt, lgh->nallocs);

  Z_TRACE_IF(LGH_TRACE_IF, "free seg: %p: [%" lghint_fmt ", %" lghint_fmt"]", seg, seg->offset, seg->size);

  prev = NULL;
  curr = lgh->segments;

/*  if (local_heap_free_search)*/
  while (curr != NULL && curr->offset < seg->offset)
  {
    prev = curr;
    curr = curr->next;
  }

  if (curr != NULL && seg->offset + seg->size == curr->offset)
  {
    curr->offset -= seg->size;
    curr->size += seg->size;

    z_free(seg);

    seg = curr;

  } else seg->next = curr;

  if (prev == NULL) lgh->segments = seg;
  else
  {
    if (prev->offset + prev->size == seg->offset)
    {
      prev->size += seg->size;
      prev->next = seg->next;

      z_free(seg);

    } else prev->next = seg;
  }

  --lgh->nallocs;

  Z_TRACE_IF(LGH_TRACE_IF, "nallocs OUT: %" lghint_fmt, lgh->nallocs);
}


#undef LGH_TRACE_IF
