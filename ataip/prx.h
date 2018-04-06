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


#ifndef __PRX_H__
#define __PRX_H__


#include "prx_conf.h"


#ifdef PRX_RENAME
# include "prx_rename.h"
#endif


typedef enum {
  PRX_FISHER_YATES_SHUFFLE,
  PRX_POW2_LCG_SHUFFLE, /* TODO */

} prx_type_t;



void prx_seed(prxint_t seed);

void prx_permutation(prxint_t *permutation, prxint_t n, prx_type_t type);

typedef struct _prx_enumerate_t *prx_enumerate_t;
#define PRX_ENUMERATE_NULL  NULL

void prx_enumerate_create(prx_enumerate_t *enumerate, prxint_t n, prx_type_t type);
void prx_enumerate_destroy(prx_enumerate_t *enumerate);
void prx_enumerate_print(prx_enumerate_t enumerate);
prxint_t prx_enumerate(prx_enumerate_t enumerate, prxint_t i);


#endif /* __PRX_H__ */
