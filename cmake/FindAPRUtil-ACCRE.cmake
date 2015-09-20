# Locate apr-util include paths and libraries. Based on findapr.cmake;
# simple modifications to apply to apr-util instead.

# This module defines
# APU_INCLUDES, where to find apu.h, etc.
# APU_LIBS, linker switches to use with ld to link against apr-util
# APU_EXTRALIBS, additional libraries to link against
# APU_LDFLAGS, additional linker flags that must be used
# APU_FOUND, set to TRUE if found, FALSE otherwise
# APU_VERSION, set to the version of apr-util found

set(APU_FOUND FALSE)

find_program(APU_CONFIG_EXECUTABLE apu-ACCRE-1-config)
mark_as_advanced(APU_CONFIG_EXECUTABLE)

macro(_apu_invoke _varname _regexp)
    execute_process(
        COMMAND ${APU_CONFIG_EXECUTABLE} ${ARGN}
        OUTPUT_VARIABLE _apu_output
        RESULT_VARIABLE _apu_failed
    )

    if(_apu_failed)
        message(FATAL_ERROR "${APU_CONFIG_EXECUTABLE} ${ARGN} failed")
    else()
        string(REGEX REPLACE "[\r\n]"  "" _apu_output "${_apu_output}")
        string(REGEX REPLACE " +$"     "" _apu_output "${_apu_output}")

        if(NOT ${_regexp} STREQUAL "")
            string(REGEX REPLACE "${_regexp}" " " _apu_output "${_apu_output}")
        endif()

        # XXX: We don't want to invoke separate_arguments() for APU_LDFLAGS;
        # just leave as-is
        if(NOT ${_varname} STREQUAL "APU_LDFLAGS")
            separate_arguments(_apu_output)
        endif()

        set(${_varname} "${_apu_output}")
    endif()
endmacro(_apu_invoke)

_apu_invoke(APU_INCLUDES  "(^| )-I" --includes)
_apu_invoke(APU_EXTRALIBS "(^| )-l" --libs)
_apu_invoke(APU_LIBTOOL   ""        --link-libtool)
_apu_invoke(APU_LIBS      ""        --link-ld)
_apu_invoke(APU_LDFLAGS   ""        --ldflags)
_apu_invoke(APU_VERSION   ""        --version)

get_filename_component(APU_LIBTOOL_BASE ${APU_LIBTOOL} PATH ) 
FIND_LIBRARY(APU_LIBRARY NAMES aprutil-ACCRE-1 PATHS ${APU_LIBTOOL_BASE})

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(APU DEFAULT_MSG APU_INCLUDES APU_LIBS APU_LIBRARY APU_VERSION)
