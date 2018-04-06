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


#include "z_pack.h"

#include "zmpi_local.h"
#include "zmpil_simple.h"


int zmpil_simple_create_derived(zmpil_simple_t *mpil, MPI_Datatype type, int count) /* zmpi_func zmpil_simple_create_derived */
{
/*  mpil->type = type;*/

/*  Z_TRACE("zmpil_simple_create_derived");*/

  MPI_Type_get_true_extent(type, &mpil->true_lb, &mpil->true_extent);

  mpil->true_extent *= count;

  Z_ASSERT(mpil->true_lb == 0);

  return 0;
}


void zmpil_simple_destroy(zmpil_simple_t *mpil) /* zmpi_func zmpil_simple_destroy */
{
}
