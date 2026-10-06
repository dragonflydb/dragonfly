# The AOF segment writer uses io_uring files, available only on Linux.
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
  SET(DF_AOF_LINUX_SRCS journal/aof_segment_writer.cc journal/aof_streamer.cc
      journal/aof_chain_reader.cc)
endif()

SET(DF_JOURNAL_SRCS
    journal/cmd_serializer.cc journal/tx_executor.cc namespaces.cc
    journal/journal.cc journal/types.cc journal/journal_slice.cc
    journal/serializer.cc journal/executor.cc journal/streamer.cc
    journal/buffered_socket_writer.cc journal/journal_applier.cc journal/aof.cc
    ${DF_AOF_LINUX_SRCS}
    PARENT_SCOPE)
