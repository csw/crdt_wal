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
      ?setup(fun passive_add/1)},
    {"Add is idempotent",
      ?setup(fun dup_add_1/1)},
    {"Add is idempotent after a restart",
      ?setup(fun dup_add_restart/1)},
    {"Add is not idempotent after an ack",
      ?setup(fun dup_add_ack/1)},
    {"Add is not idempotent after an ack/restart",
      ?setup(fun dup_add_ack_restart/1)},
    {"Duplicate add increments version",
      ?setup(fun dup_add_2/1)}].

%% checkpoint_test_() ->
%%     [{"Checkpoints work",
%%       ?setup(fun run_checkpoint_1/1)}].

start() ->
    Dir = "/tmp/passive",
    ?assertCmd("sh -c 'if [ -e /tmp/passive ]; then rm -r /tmp/passive; fi'"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    ok = application:set_env(crdt_wal, actor, test_a1),
    ok = application:set_env(crdt_wal, data_dir, Dir),
    ok = application:set_env(crdt_wal, log_dir, Dir),
    %%ok = start_app(sasl),
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

dup_add_1(_) ->
    Key = "abc",
    RequestID = {70,1},
    Elt = "ball bearings",
    {ok, _B0, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, B1, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    {ok, B2, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    [?_assertEqual(crdt_entry(Elt, B1), crdt_entry(Elt, B2))].
    
dup_add_restart(_) ->
    Key = "abc",
    RequestID = {70,1},
    Elt = "ball bearings",
    {ok, _B0, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, B1, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    ok = restart(),
    {ok, B2, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    [?_assertEqual(crdt_entry(Elt, B1), crdt_entry(Elt, B2))].
    
dup_add_ack(_) ->
    Key = "abc",
    RequestID = {70,1},
    Elt = "ball bearings",
    {ok, _B0, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, B1, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    ok = crdt_service:acknowledge(crdt_service, RequestID),
    %% We GC these instead of deleting them immediately.
    wal_mgr:start_req_checkpoint(),
    timer:sleep(timer:seconds(2)),
    %% should now be removed from outstanding_lsn
    wal_mgr:take_checkpoint(),
    timer:sleep(timer:seconds(2)),
    %% recovery LSN should now be past it
    wal_mgr:start_req_checkpoint(),
    timer:sleep(timer:seconds(3)),
    %% should now be GC'd from requests
    {ok, B2, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    [?_assertNotEqual(crdt_entry(Elt, B1), crdt_entry(Elt, B2))].

dup_add_ack_restart(_) ->
    Key = "abc",
    RequestID = {70,1},
    Elt = "ball bearings",
    {ok, _B0, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, B1, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    ok = crdt_service:acknowledge(crdt_service, RequestID),
    ok = restart(),
    {ok, B2, _} = crdt_service:passive_op(crdt_service, Key, RequestID,
                                          {add, Elt}),
    [?_assertNotEqual(crdt_entry(Elt, B1), crdt_entry(Elt, B2))].

dup_add_2(_) ->
    Key = "abc",
    Elt = "ball bearings",
    {ok, _B0, _} = crdt_service:create(crdt_service, Key, ?SM),
    {ok, B1, _} = crdt_service:passive_op(crdt_service, Key, {70,1},
                                          {add, Elt}),
    {ok, B2, _} = crdt_service:passive_op(crdt_service, Key, {70,2},
                                          {add, Elt}),
    [?_assertNotEqual(crdt_entry(Elt, B1), crdt_entry(Elt, B2))].


restart() ->
    ok = application:stop(crdt_wal),
    ok = application:start(crdt_wal),
    ok.

crdt_entry(Key, BSet) ->
    {_Clock, Entries} = ?SM:from_binary(BSet),
    orddict:find(Key, Entries).

-endif.

