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

#include "z_pack.h"
#include "prx.h"


void prx_seed(prxint_t seed) /* prx_func prx_seed */
{
  z_srand(seed);
}


void prx_permutation(prxint_t *permutation, prxint_t n, prx_type_t type) /* prx_func prx_permutation */
{
  prxint_t i, j, t;


  switch (type)
  {
    case PRX_FISHER_YATES_SHUFFLE:
      for (i = 0; i < n; ++i) permutation[i] = i;
      for (i = n - 1; i > 0; --i)
      {
        j = (prxint_t) z_rand_minmax(0, i);
        t = permutation[i]; permutation[i] = permutation[j]; permutation[j] = t;
      }
      break;

    default:
      fprintf(stderr, "ERROR: unknown prx type '%d'\n", (int) type);
  }
}


struct _prx_enumerate_t
{
  prx_type_t type;
  prxint_t n;
  prxint_t *permutation;
};


void prx_enumerate_create(prx_enumerate_t *enumerate, prxint_t n, prx_type_t type) /* prx_func prx_enumerate_create */
{
  *enumerate = z_alloc(1, sizeof(**enumerate));

  (*enumerate)->type = type;
  (*enumerate)->n = n;
  (*enumerate)->permutation = NULL;

  switch (type)
  {
    case PRX_FISHER_YATES_SHUFFLE:
      (*enumerate)->permutation = z_alloc(n, sizeof(prxint_t));
      prx_permutation((*enumerate)->permutation, n, type);
      break;

    default:
      fprintf(stderr, "ERROR: unknown prx type '%d'\n", (int) type);
      prx_enumerate_destroy(enumerate);
      return;
  }

/*  prx_enumerate_print(*enumerate);*/
}


void prx_enumerate_destroy(prx_enumerate_t *enumerate) /* prx_func prx_enumerate_destroy */
{
  if (enumerate == PRX_ENUMERATE_NULL) return;

  if ((*enumerate)->permutation) z_free((*enumerate)->permutation);

  z_free(*enumerate);

  *enumerate = PRX_ENUMERATE_NULL;
}


void prx_enumerate_print(prx_enumerate_t enumerate) /* prx_func prx_enumerate_print */
{
  prxint_t i;


  printf("enumerate %p:\n", enumerate);
  for (i = 0; i < enumerate->n; ++i) printf("  %" prxint_fmt " -> %" prxint_fmt "\n", i, prx_enumerate(enumerate, i));
}


prxint_t prx_enumerate(prx_enumerate_t enumerate, prxint_t i) /* prx_func prx_enumerate */
{
  switch (enumerate->type)
  {
    case PRX_FISHER_YATES_SHUFFLE:
      return enumerate->permutation[i];

    default:
      fprintf(stderr, "ERROR: unknown prx type '%d'\n", (int) enumerate->type);
  }

  return -1;
}
