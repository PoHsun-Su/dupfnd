cmake_minimum_required(VERSION 3.9)
PROJECT (DUPFND_SRC)

SET(MY_TARGET dupfnd)
SET(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/libs)
SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../include)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -O2 -w -Wall -pedantic")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O2 -w -Wall -pedantic")

add_definitions(-D_LARGEFILE64_SOURCE)
add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-DDUPFND_THREAD)
# add_definitions(-DEXCLUDE_RULES)

include(CheckCCompilerFlag)
check_c_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    add_definitions(-DSSE_CRC32)
endif()

ADD_EXECUTABLE ("${MY_TARGET}" "${CMAKE_CURRENT_SOURCE_DIR}/main.c" "${CMAKE_CURRENT_SOURCE_DIR}/dupfnd.c" "${CMAKE_CURRENT_SOURCE_DIR}/rbtree.c" "${CMAKE_CURRENT_SOURCE_DIR}/util.c")

TARGET_LINK_LIBRARIES("${MY_TARGET}" dl m pthread z)

