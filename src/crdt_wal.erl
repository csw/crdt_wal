-module(crdt_wal).

-export([start/0]).

start() ->
    ok = application:start(sasl),
    ok = application:start(crypto),
    ok = application:start(riak_dt),
    ok = application:start(folsom),
    application:start(crdt_wal).
