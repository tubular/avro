# Tries to find LZ4 headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(LZ4)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  LZ4_ROOT_DIR  Set this variable to the root installation of
#                    LZ4 if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  LZ4_FOUND              System has LZ4 libs/headers
#  LZ4_LIBRARIES          The LZ4 libraries
#  LZ4_INCLUDE_DIR        The location of LZ4 headers

find_path(LZ4_INCLUDE_DIR
    NAMES lz4.h
    HINTS ${LZ4_ROOT_DIR}/include)

find_library(LZ4_LIBRARIES
    NAMES lz4
    HINTS ${LZ4_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG
    LZ4_LIBRARIES
    LZ4_INCLUDE_DIR)

mark_as_advanced(
    LZ4_ROOT_DIR
    LZ4_LIBRARIES
    LZ4_INCLUDE_DIR)
