SET(DF_JOURNAL_SRCS
    journal/cmd_serializer.cc journal/tx_executor.cc namespaces.cc
    journal/journal.cc journal/types.cc journal/journal_slice.cc
    journal/serializer.cc journal/executor.cc journal/streamer.cc
    journal/buffered_socket_writer.cc
    PARENT_SCOPE)
