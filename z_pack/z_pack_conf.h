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


#ifndef __Z_PACK_CONF_H__
#define __Z_PACK_CONF_H__


typedef long z_int_t;
#define z_int_fmt  "ld"

#define Z_PACK_RENAME
#define Z_PREFIX  zmpi_ataip_

/*#define Z_PACK_MPI
#define Z_PACK_MPI_RANK  z_mpi_rank
extern int z_mpi_rank;*/

#define z_mpi_rank  0

#define Z_PACK_NUMERIC

#define Z_PACK_DEBUG

#define Z_PACK_TIME

#define Z_PACK_ALLOC

#define Z_PACK_RANDOM


#endif /* __Z_PACK_CONF_H__ */
