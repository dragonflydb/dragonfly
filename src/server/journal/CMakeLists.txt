add_library(dfly_journal
    cmd_serializer.cc tx_executor.cc
    journal.cc types.cc journal_slice.cc
    serializer.cc executor.cc streamer.cc)
target_link_libraries(dfly_journal dfly_transaction dragonfly_lib dfly_facade base io)

helio_cxx_test(journal_test dfly_test_lib LABELS DFLY)
add_dependencies(check_dfly journal_test)
