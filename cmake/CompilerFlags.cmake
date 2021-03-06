MACRO (CompilerFlags RESULT)
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
       set(${RESULT} "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_DEBUG}")
    elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
       set(${RESULT} "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_RELEASE}")
    else ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
       set(${RESULT} "${CMAKE_C_FLAGS}")
    endif ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
ENDMACRO (CompilerFlags)

