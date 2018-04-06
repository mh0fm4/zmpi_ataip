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
#include "prx.h"


/*#define QSORT*/

#ifdef QSORT
static int compare_dsints(const void *b0, const void *b1)
{
  return *((dsint_t *) b0) - *((dsint_t *) b1);
}
#endif


void ds_sort_dsints(dsint_t *ints, dsint_t n, dsint_t x) /* ds_func ds_sort_dsints */
{
#ifdef QSORT

  qsort(ints, n, x * sizeof(dsint_t), compare_dsints);

#else

  dsint_t k, t;
#define GET(_i_)        (ints[(_i_) * x])
#define XCHG(_i_, _j_)  Z_MOP(for (k = 0; k < x; ++k) { t = ints[(_i_) * x + k]; ints[(_i_) * x + k] = ints[(_j_) * x + k]; ints[(_j_) * x + k] = t; })

  DASH_RADIXSORT_SORT_DECLARE(dsint_t)

  DASH_RADIXSORT_SORT(n, 0, sizeof(dsint_t) * 8 - 1, GET, XCHG);

#undef GET
#undef XCHG

#endif
}

#undef QSORT
