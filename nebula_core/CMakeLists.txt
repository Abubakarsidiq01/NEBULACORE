cmake_minimum_required(VERSION 3.15)
project(nebula_core)

set(CMAKE_CXX_STANDARD 17)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 OLD)
endif()

find_package(Boost REQUIRED COMPONENTS system)

# Main library
add_library(nebula_core
    src/nebula_log.cpp
    src/nebula_wire.cpp
    src/segment.cpp
    src/topic_manager.cpp
    src/cluster.cpp
    src/leader.cpp
)

target_include_directories(nebula_core PUBLIC src)

# Core linking for Windows + Boost.Asio
target_link_libraries(nebula_core 
    PRIVATE 
        Boost::system
        ws2_32
        mswsock
)

# Test 1
add_executable(test_nebula_log tests/test_nebula_log.cpp)
target_link_libraries(test_nebula_log 
    PRIVATE 
        nebula_core
        Boost::system 
        ws2_32 
        mswsock
)

# Test 2
add_executable(test_nebula_wire tests/test_nebula_wire.cpp)
target_link_libraries(test_nebula_wire
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 3
add_executable(test_topics tests/test_topics.cpp)
target_link_libraries(test_topics
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 4
add_executable(test_gossip tests/test_gossip.cpp)
target_link_libraries(test_gossip
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 5
add_executable(test_leader tests/test_leader.cpp)
target_link_libraries(test_leader
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

