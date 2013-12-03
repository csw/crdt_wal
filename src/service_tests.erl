-module(service_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(SM, logged_orswot).

-define(setup(F), {setup, fun start/0, fun stop/1, F}).

simple_test_() ->
    [{"An empty set can be created",
      ?setup(fun create_empty/1)},
     {"An empty set can be fetched after recovery",
      ?setup(fun fetch_after_recovery/1)},
    {"A passive add works after recovery",
      ?setup(fun passive_add/1)}].

start() ->
    Dir = "/tmp/passive",
    ?assertCmd("sh -c 'if [ -e /tmp/passive ]; then rm -r /tmp/passive; fi'"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = application:set_env(crdt_wal, actor, test_a1),
    ok = application:set_env(crdt_wal, data_dir, Dir),
    ok = application:set_env(crdt_wal, log_dir, Dir),
    ok = start_app(sasl),
    ok = start_app(crdt_wal).

start_app(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok;
        {error, {not_started, OtherApp}} ->
            ok = start_app(OtherApp),
            start_app(App)
    end.

stop(_) ->
    case application:stop(crdt_wal) of
        ok                             -> ok;
        {error,{not_started,crdt_wal}} -> ok
    end.

create_empty(_) ->
    {ok, BSet, _} = crdt_service:create(crdt_service, "abc", ?SM),
    Set = ?SM:from_binary(BSet),
    [?_assertEqual([], ?SM:value(Set))].
    
fetch_after_recovery(_) ->
    Key = "abc",
    {ok, _BSet, _} = crdt_service:create(crdt_service, Key, ?SM),
    ok = restart(),
    {ok, BSet, _} = crdt_service:fetch(crdt_service, Key),
    Set = ?SM:from_binary(BSet),
    [?_assertEqual([], ?SM:value(Set))].

passive_add(_) ->
    Key = "abc",
    {ok, _BSet, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, _, _} = crdt_service:passive_op(crdt_service, Key, {70,1},
                                         {add, "ball bearings"}),
    ok = restart(),
    {ok, BSet, _} = crdt_service:fetch(crdt_service, Key),
    Set = ?SM:from_binary(BSet),
    [?_assertEqual(["ball bearings"], ?SM:value(Set))].

restart() ->
    ok = application:stop(crdt_wal),
    ok = application:start(crdt_wal),
    ok.



-endif.

