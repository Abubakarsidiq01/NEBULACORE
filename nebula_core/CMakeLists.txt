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
    src/nebula_node.cpp
    src/node_server.cpp
    src/raft.cpp
    src/raft_server.cpp
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

# Test 6
add_executable(test_node tests/test_node.cpp)
target_link_libraries(test_node
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 7
add_executable(test_node_server tests/test_node_server.cpp)
target_link_libraries(test_node_server
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 8
add_executable(test_replication tests/test_replication.cpp)
target_link_libraries(test_replication
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 9
add_executable(test_commit_index tests/test_commit_index.cpp)
target_link_libraries(test_commit_index
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 10
add_executable(test_wal_recovery tests/test_wal_recovery.cpp)
target_link_libraries(test_wal_recovery
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 11 RAFT basic timer test
add_executable(test_raft_phase1 tests/test_raft_phase1.cpp)
target_link_libraries(test_raft_phase1
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 12 RAFT RequestVote RPC + Real Leader Election test
add_executable(test_raft_phase2 tests/test_raft_phase2.cpp)
target_link_libraries(test_raft_phase2
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 13 RAFT log replication test
add_executable(test_raft_phase3 tests/test_raft_phase3.cpp)
target_link_libraries(test_raft_phase3
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 14 RAFT commit index test
add_executable(test_raft_phase4 tests/test_raft_phase4.cpp)
target_link_libraries(test_raft_phase4
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# Test 15 RAFT Stability
add_executable(test_raft_phase5 tests/test_raft_phase5.cpp)
target_link_libraries(test_raft_phase5
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

#Test 16 RAFT Log Compaction + Snapshots
add_executable(test_raft_phase6 tests/test_raft_phase6.cpp)
target_link_libraries(test_raft_phase6
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

#Test 17 RAFT
add_executable(test_raft_phase7 tests/test_raft_phase7.cpp)
target_link_libraries(test_raft_phase7
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

#Test 18 RAFT Snapshot + Installation
add_executable(test_raft_phase8 tests/test_raft_phase8.cpp)
target_link_libraries(test_raft_phase8
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

#Test 19 RAFT Network Basic Test
add_executable(test_raft_network_basic tests/test_raft_network_basic.cpp)
target_link_libraries(test_raft_network_basic
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)

# -------------------------------------------------------------------
# NEBULA NODE MAIN (Real Multi-Process Raft Cluster Entry Point)
# -------------------------------------------------------------------
add_executable(nebula_node_main
    src/nebula_node_main.cpp
)

target_link_libraries(nebula_node_main
    PRIVATE
        nebula_core
        Boost::system
        ws2_32
        mswsock
)
