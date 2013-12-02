-ifndef(LOG_HEADER_PB_H).
-define(LOG_HEADER_PB_H, true).
-record(log_header, {
    version = erlang:error({required, version}),
    start_lsn
}).
-endif.

-ifndef(LOG_REC_PB_H).
-define(LOG_REC_PB_H, true).
-record(log_rec, {
    lsn = erlang:error({required, lsn}),
    tstamp = erlang:error({required, tstamp}),
    tx
}).
-endif.

-ifndef(TX_REC_PB_H).
-define(TX_REC_PB_H, true).
-record(tx_rec, {
    bucket,
    key = erlang:error({required, key}),
    operations = erlang:error({required, operations})
}).
-endif.

-ifndef(CHECKPOINT_REC_PB_H).
-define(CHECKPOINT_REC_PB_H, true).
-record(checkpoint_rec, {
    tstamp = erlang:error({required, tstamp}),
    recovery_lsn = erlang:error({required, recovery_lsn}),
    last_known_lsn = erlang:error({required, last_known_lsn}),
    clock_file = erlang:error({required, clock_file})
}).
-endif.

