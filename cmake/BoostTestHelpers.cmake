function(add_boost_test SOURCE_FILE_NAME)
    get_filename_component(TEST_EXECUTABLE_NAME ${SOURCE_FILE_NAME} NAME_WE)


    add_executable(${TEST_EXECUTABLE_NAME} ${SOURCE_FILE_NAME})

    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set_target_properties(${TEST_EXECUTABLE_NAME} PROPERTIES COMPILE_FLAGS ${GCC_CXX_FLAGS})
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
        set_target_properties(${TEST_EXECUTABLE_NAME} PROPERTIES COMPILE_FLAGS ${MSVC_CXX_FLAGS})
    endif()

    target_link_libraries(${TEST_EXECUTABLE_NAME} Boost::unit_test_framework)
    
    foreach(DEPENDENCY_LIB IN LISTS ARGN)
        target_link_libraries(${TEST_EXECUTABLE_NAME} ${DEPENDENCY_LIB})
    endforeach()

    file(READ "${SOURCE_FILE_NAME}" SOURCE_FILE_CONTENTS)
    string(REGEX MATCHALL "BOOST_AUTO_TEST_CASE\\( *([A-Za-z_0-9]+) *\\)" 
           FOUND_TESTS ${SOURCE_FILE_CONTENTS})

    foreach(HIT ${FOUND_TESTS})
        string(REGEX REPLACE ".*\\( *([A-Za-z_0-9]+) *\\).*" "\\1" TEST_NAME ${HIT})

        add_test(NAME "${TEST_EXECUTABLE_NAME}.${TEST_NAME}" 
                 COMMAND ${TEST_EXECUTABLE_NAME}
                 --run_test=${TEST_NAME} --catch_system_error=yes)
    endforeach()
endfunction()
