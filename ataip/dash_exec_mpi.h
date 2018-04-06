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


#ifndef __DASH_EXEC_MPI_H__
#define __DASH_EXEC_MPI_H__


#include <mpi.h>

#include "zmpi_local.h"

#include "dash_core.h"


typedef struct _ds_exec_mpi_t
{
  dsint_t ntypes;
  MPI_Datatype mpi_types[DASH_MAX_NBUFFERS];
  zmpil_t zmpil_types[DASH_MAX_NBUFFERS];

  dsint_t nmax, n;
  MPI_Request *reqs;
  MPI_Status *stats;

  int *scounts, *sdispls, *rcounts, *rdispls;
  MPI_Datatype *stypes, *rtypes;
  MPI_Datatype *addr_types;

#if defined(DASH_SYMMETRIC) && defined(DASH_SYMMETRIC_AUX)
  dsint_t sym_aux_nreqs;
#endif

} ds_exec_mpi_t, *ds_exec_mpi_p;

#define DEFINE_EXEC_MPI(_s_, _v_)  ds_exec_mpi_t *_v_ = (_s_)->cxt

#define DS_EXEC_MPI_ISENDRECV_TAG         0
#define DS_EXEC_MPI_SENDRECV_REPLACE_TAG  0
#define DS_EXEC_MPI_SENDRECV_AUX_TAG      0


dsint_t ds_exec_mpi_create(ds_exec_t *exec);
dsint_t ds_exec_mpi_destroy(ds_exec_t *exec);

dsint_t ds_exec_mpi_add_address(ds_exec_t *exec, void *addr);

dsint_t ds_exec_mpi_add_type(ds_exec_t *exec, MPI_Datatype type);
dsint_t ds_exec_mpi_sizefor(ds_exec_t *exec, dsint_t exec_id, dsint_t size);
dsint_t ds_exec_mpi_extent(ds_exec_t *exec, dsint_t exec_id);


#endif /* __DASH_EXEC_MPI_H__ */
