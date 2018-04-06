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


#ifndef __LOCAL_GENERIC_HEAP_RENAME_H__
#define __LOCAL_GENERIC_HEAP_RENAME_H__


#define LGH_CONCAT(_a_, _b_)           LGH_CONCAT_(_a_, _b_)
#define LGH_CONCAT_(_a_, _b_)          _a_##_b_

#define LGH_CONCONCAT(_a_, _b_, _c_)   LGH_CONCONCAT_(_a_, _b_, _c_)
#define LGH_CONCONCAT_(_a_, _b_, _c_)  _a_##_b_##_c_

#ifdef LGH_PREFIX
# define LGH_VAR(_v_)   LGH_CONCAT(LGH_PREFIX, _v_)
# define LGH_FUNC(_f_)  LGH_CONCAT(LGH_PREFIX, _f_)
#else
# define LGH_VAR(_v_)   _v_
# define LGH_FUNC(_f_)  _f_
#endif


/* local_generic_heap.c */
#define lgh_create  LGH_FUNC(lgh_create)
#define lgh_destroy  LGH_FUNC(lgh_destroy)
#define lgh_alloc  LGH_FUNC(lgh_alloc)
#define lgh_alloc_minmax  LGH_FUNC(lgh_alloc_minmax)
#define lgh_free  LGH_FUNC(lgh_free)


#endif /* __LOCAL_GENERIC_HEAP_RENAME_H__ */
