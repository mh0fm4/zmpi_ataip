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


#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "zmpi_ataip.h"


/*#define PRINT*/
#define BENCH
#define VERIFY


int qsort_int_cmp(const void *b0, const void *b1)
{
  return *((int *) b0) - *((int *) b1);
}


int main(int argc, char **argv)
{
  const int stotal = 1000000;
  int rtotal;

  int i, j;
  int *buf;
  int *scounts, *sdispls, *rcounts, *rdispls;

  MPI_Comm comm;
  int comm_size, comm_rank;
  
  double t[3];


  MPI_Init(&argc, &argv);

  comm = MPI_COMM_WORLD;

  MPI_Comm_size(comm, &comm_size);
  MPI_Comm_rank(comm, &comm_rank);

  scounts = malloc(comm_size * 4 * sizeof(int));
  sdispls = scounts + 1 * comm_size;
  rcounts = scounts + 2 * comm_size;
  rdispls = scounts + 3 * comm_size;

  srandom((comm_rank + 1) * 2501);

  sdispls[0] = 0;
  for (i = 1; i < comm_size; ++i) sdispls[i] = random() % stotal;

  qsort(sdispls, comm_size, sizeof(int), qsort_int_cmp);

  for (i = 0; i < comm_size - 1; ++i) scounts[i] = sdispls[i + 1] - sdispls[i];
  scounts[i] = stotal - sdispls[i];

  MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, comm);

  rdispls[0] = 0;
  for (i = 1; i < comm_size; ++i) rdispls[i] = rdispls[i - 1] + rcounts[i - 1];

  rtotal = rdispls[comm_size - 1] + rcounts[comm_size - 1];

  buf = malloc(((stotal < rtotal)?rtotal:stotal) * sizeof(int));

  for (i = 0; i < comm_size; ++i)
  for (j = sdispls[i]; j < sdispls[i] + scounts[i]; ++j) buf[j] = i;

#ifdef PRINT
  printf("%d: input\n", comm_rank);
  for (i = 0; i < stotal; ++i) printf(" %d: %d\n", i, buf[i]);
#endif

  ZMPI_Alltoallv_inplace_aux = NULL;
  ZMPI_Alltoallv_inplace_aux_size = 0.01 * stotal * sizeof(int);

  MPI_Barrier(comm);
  t[0] = MPI_Wtime();
  ZMPI_Alltoallv_inplace(MPI_IN_PLACE, scounts, sdispls, MPI_INT, buf, rcounts, rdispls, MPI_INT, comm);
  MPI_Barrier(comm);
  t[0] = MPI_Wtime() - t[0];

#ifdef VERIFY
  for (i = 0; i < rtotal; ++i)
  if (buf[i] != comm_rank) break;

  if (i >= rtotal) printf("%d: verify OK\n", comm_rank);
  else printf("%d: verify FAILED at %d\n", comm_rank, i);
#endif

#ifdef BENCH
  for (i = 0; i < comm_size; ++i)
  for (j = sdispls[i]; j < sdispls[i] + scounts[i]; ++j) buf[j] = i;

  ZMPI_Alltoallv_inplace_aux_type = ZMPI_ALLTOALLV_INPLACE_AUX_TYPE_STATIC;

  MPI_Barrier(comm);
  t[1] = MPI_Wtime();
  ZMPI_Alltoallv_inplace(MPI_IN_PLACE, scounts, sdispls, MPI_INT, buf, rcounts, rdispls, MPI_INT, comm);
  MPI_Barrier(comm);
  t[1] = MPI_Wtime() - t[1];

  for (i = 0; i < comm_size; ++i)
  for (j = sdispls[i]; j < sdispls[i] + scounts[i]; ++j) buf[j] = i;

  ZMPI_Alltoallv_inplace_aux_type = ZMPI_ALLTOALLV_INPLACE_AUX_TYPE_HEAP;

  MPI_Barrier(comm);
  t[2] = MPI_Wtime();
  ZMPI_Alltoallv_inplace(MPI_IN_PLACE, scounts, sdispls, MPI_INT, buf, rcounts, rdispls, MPI_INT, comm);
  MPI_Barrier(comm);
  t[2] = MPI_Wtime() - t[2];

  if (comm_rank == 0) printf("%d: default: %f, static: %f, heap: %f\n", comm_rank, t[0], t[1], t[2]);
#endif

#ifdef PRINT
  printf("%d: output\n", comm_rank);
  for (i = 0; i < rtotal; ++i) printf(" %d: %d\n", i, buf[i]);
#endif

  free(buf);

  free(scounts);

  MPI_Finalize();

  return 0;
}
