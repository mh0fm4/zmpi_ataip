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


#ifndef __LOCAL_GENERIC_HEAP_CONF_H__
#define __LOCAL_GENERIC_HEAP_CONF_H__


#include "dash_conf.h"


#define LGH_RENAME

#ifdef ZMPI_PREFIX
# define LGH_PREFIX  LGH_CONCAT(ZMPI_PREFIX, zmpi_)
#else
# define LGH_PREFIX  zmpi_
#endif


typedef dsint_t lghint_t;
#define lghint_fmt  dsint_fmt


#endif /* __LOCAL_GENERIC_HEAP_CONF_H__ */
