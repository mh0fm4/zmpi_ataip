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


#ifndef __ZMPI_LOCAL_H__
#define __ZMPI_LOCAL_H__


#include "zmpi_local_conf.h"

#ifdef ZMPI_PREFIX
# include "zmpi_local_rename.h"
#endif


#if defined(ZMPI_LOCAL_SIMPLE)

# include "zmpil_simple.h"

# define zmpil_t                                                         zmpil_simple_t
# define zmpil_create                                                    zmpil_simple_create
# define zmpil_destroy                                                   zmpil_simple_destroy
# define zmpil_at(_b_, _n_, _m_)                                         zmpil_simple_at(_b_, _n_, _m_)
# define zmpil_diff(_bfrom_, _bto_, _m_)                                 zmpil_simple_diff(_bfrom_, _bto_, _m_)  
# define zmpil_extent(_m_)                                               zmpil_simple_extent(_m_)
# define zmpil_nextent(_n_, _m_)                                         zmpil_simple_nextent(_n_, _m_)
# define zmpil_sizefor(_s_, _m_)                                         zmpil_simple_sizefor(_s_, _m_)
# define zmpil_copy(_s_, _d_)                                            zmpil_simple_copy(_s_, _d_)
# define zmpil_memcpy(_d_, _s_, _n_, _m_)                                zmpil_simple_memcpy(_d_, _s_, _n_, _m_) 
# define zmpil_memmove(_d_, _s_, _n_, _m_)                               zmpil_simple_memmove(_d_, _s_, _n_, _m_) 
# define zmpil_memcpy_at(_d_, _dat_, _s_, _sat_, _n_, _m_)               zmpil_simple_memcpy_at(_d_, _dat_, _s_, _sat_, _n_, _m_)
# define zmpil_memmove_at(_d_, _dat_, _s_, _sat_, _n_, _m_)              zmpil_simple_memmove_at(_d_, _dat_, _s_, _sat_, _n_, _m_)
# define zmpil_memcpy_conv(_d_, _s_, _n_, _md_, _ms_)                    zmpil_simple_memcpy_conv(_d_, _s_, _n_, _md_, _ms_)
# define zmpil_memmove_conv(_d_, _s_, _n_, _md_, _ms_)                   zmpil_simple_memmove_conv(_d_, _s_, _n_, _md_, _ms_)
# define zmpil_memcpy_conv_at(_d_, _dat_, _s_, _sat_, _n_, _md_, _ms_)   zmpil_simple_memcpy_conv_at(_d_, _dat_, _s_, _sat_, _n_, _md_, _ms_)
# define zmpil_memmove_conv_at(_d_, _dat_, _s_, _sat_, _n_, _md_, _ms_)  zmpil_simple_memmove_conv_at(_d_, _dat_, _s_, _sat_, _n_, _md_, _ms_)

#elif defined(ZMPI_LOCAL_MPITYPES)

# error support for MPITypes not implemented, yet!

#else

# error ZMPI-Local method unknown!

#endif


#endif /* __ZMPI_LOCAL_H__ */
