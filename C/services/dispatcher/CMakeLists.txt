cmake_minimum_required(VERSION 2.6.0)

# Set the plugin name to build
project("Dispatcher server")
set(EXEC fledge.services.dispatcher)
set(DLLIB -ldl)

# Supported options:
# -DFLEDGE_INCLUDE
# -DFLEDGE_LIB
# -DFLEDGE_SRC
# -DFLEDGE_INSTALL
#
# If no -D options are given and FLEDGE_ROOT environment variable is set
# then Fledge libraries and header files are pulled from FLEDGE_ROOT path.

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -ggdb")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3")

# Add here all needed Fledge libraries as list
set(NEEDED_FLEDGE_LIBS common-lib services-common-lib filters-common-lib)

find_package(Threads REQUIRED)

set(BOOST_COMPONENTS system thread)
# Late 2017 TODO: remove the following checks and always use std::regex
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.9)
        set(BOOST_COMPONENTS ${BOOST_COMPONENTS} regex)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_BOOST_REGEX")
    endif()
endif()
find_package(Boost 1.53.0 COMPONENTS ${BOOST_COMPONENTS} REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIR})

if(APPLE)
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")
endif()

# Find source files
# C++ main
file(GLOB SOURCES *.cpp)
# C++ classes
file(GLOB COMPONENTS ../common/*.cpp)

# Find Fledge includes and libs, by including FindFledge.cmak file
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR})
find_package(Fledge)
# If errors: make clean and remove Makefile
if (NOT FLEDGE_FOUND)
        if (EXISTS "${CMAKE_BINARY_DIR}/Makefile")
                execute_process(COMMAND make clean WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
                file(REMOVE "${CMAKE_BINARY_DIR}/Makefile")
        endif()
        # Stop the build process
        message(FATAL_ERROR "Fledge plugin '${PROJECT_NAME}' build error.")
endif()
# On success, FLEDGE_INCLUDE_DIRS and FLEDGE_LIB_DIRS variables are set 

# Add dispatcher service common/include
include_directories(include)
include_directories(../common/include)

# Add Fledge include dir(s)
include_directories(${FLEDGE_INCLUDE_DIRS})

# Add other include paths this plugin needs
if (FLEDGE_SRC)
	message(STATUS "Using third-party includes " ${FLEDGE_SRC}/C/thirdparty/Simple-Web-Server)
	include_directories(${FLEDGE_SRC}/C/thirdparty/Simple-Web-Server)
else()
	include_directories(${FLEDGE_INCLUDE_DIRS}/Simple-Web-Server)
endif()

# Add Fledge lib path
link_directories(${FLEDGE_LIB_DIRS})
link_directories(${PROJECT_BINARY_DIR}/../../lib)

add_executable(${EXEC} ${SOURCES} ${COMPONENTS})
target_link_libraries(${EXEC} ${Boost_LIBRARIES})
target_link_libraries(${EXEC} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(${EXEC} ${DLLIB})
target_link_libraries(${EXEC} ${UUIDLIB})
target_link_libraries(${EXEC} ${NEEDED_FLEDGE_LIBS})

set(FLEDGE_INSTALL "" CACHE INTERNAL "")
# Install library
if (FLEDGE_INSTALL)
	message(STATUS "Installing ${PROJECT_NAME} in ${FLEDGE_INSTALL}/services")
	install(TARGETS ${EXEC} RUNTIME DESTINATION ${FLEDGE_INSTALL}/services)
endif()
