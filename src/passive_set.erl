%% passive_set.erl: client interface for passive Set CRDTs.

-module(passive_set).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([set/1]).

-export([new/4, fetch/4, value/1, value/2, update/2, sync/1]).

-type smod()         :: module().
-type replica_id()   :: term().
-type counter()      :: non_neg_integer().
-type mac()          :: term().

-type member()     :: term().
-type set_op()     :: {'add', member()} | {'remove', member()}.
-type request_id() :: {replica_id(), counter()}.
-type pending_op() :: {request_id(), replica_id(), term()}.

-record(set, {mod                    :: module(),
              crdt                   :: term(),
              cid                    :: term(),
              rid                    :: replica_id(),
              next_req=0             :: counter(),
              mac=none               :: mac(),
              adds=[]                :: ordsets:ordset(member()),
              rems=[]                :: ordsets:ordset(member()),
              sources=[crdt_service] :: [term()],
              pending=none           :: pending_op() | 'none'
             }).
-type set(CRDT)                      :: #set{crdt :: CRDT}.

-spec new(smod(), CRDT, term(), replica_id()) -> set(CRDT).
new(Mod, CRDT, CID, RID) ->
    #set{mod=Mod, crdt=CRDT, cid=CID, rid=RID}.

-spec fetch(string() | binary(), replica_id(), [term()], module()) -> #set{}.
fetch(Key, Replica, NodesOrig, Mod) ->
    Nodes = randomize_nodes(NodesOrig),
    S=#set{} = try_fetch(Key, Replica, Mod, Nodes, []),
    S.

try_fetch(_Key, _Replica, _Mod, [], _Tried) ->
    {error, notfound};
try_fetch(Key, Replica, Mod, [Node|NS], Tried) ->
    case crdt_service:fetch(Node, Key) of
        {ok, CBin, MAC} ->
            #set{cid=Key, mod=Mod, crdt=Mod:from_binary(CBin), mac=MAC,
                 rid=Replica, sources=[Node|NS] ++ Tried};
        {error, notfound} ->
            try_fetch(Key, Replica, Mod, NS, [Node|Tried]);
        E={error, _Reason} ->
            E
    end.

-spec value(set(_CRDT)) -> [member()].
value(#set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    Orig = Mod:value(CRDT),
    ordsets:union(ordsets:subtract(Orig, Rems), Adds).

-spec value({'contains', term()}, set(_CRDT)) -> boolean().
value({contains, Elem},
      #set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    %% in Adds or (in CRDT and not in Rems)
    ordsets:is_element(Elem, Adds)
        orelse (Mod:value({contains, Elem}, CRDT)
                andalso not ordsets:is_element(Elem, Rems)).


-spec update(set_op(), set(CRDT)) -> set(CRDT).
update({add, E}, S=#set{adds=Adds, rems=Rems, pending=none}) ->
    S#set{adds=ordsets:add_element(E, Adds),
          rems=ordsets:del_element(E, Rems)};
update({remove, E},
       S=#set{mod=CMod, crdt=CRDT, adds=Adds, rems=Rems, pending=none}) ->
    case CMod:value({contains, E}, CRDT) of
        true ->
            %% if in the CRDT, remove from Adds, add to Rems
            S#set{adds=ordsets:del_element(E, Adds),
                  rems=ordsets:add_element(E, Rems)};
        false ->
            %% if in Adds but not CRDT, remove from adds
            S#set{adds=ordsets:del_element(E, Adds)}
    end.

%% For riak_dt_orswot:
%%
%%  prepare({passive_add, E})             -> {add, E}
%%  prepare({passive_remove, E})          -> {remove_versions, E, Dots}
%%
%%  prepare({add, E})   -> {add_at, E, Dot}
%%  prepare({remove_versions, E, Dots}) -> {remove_versions, E, Dots}
%%
%%  effect({add_at, E, Dot})
%%  effect({remove_versions, E, Dots})
%%    (check clocks, not dot presence)
%%

-spec sync(set(CRDT)) -> {'ok', set(CRDT)} |
                         {'retry', set(CRDT)}.
sync(S=#set{adds=[], rems=[], mod=CMod, crdt=CRDT, cid=CID,
            sources=[Source|_], pending=none}) ->
    {ok, ResC, ResMAC} = crdt_service:fetch(Source, CID),
    Merged = CMod:merge(CRDT, CMod:from_binary(ResC)),
    {ok, S#set{crdt=Merged, mac=ResMAC}};
sync(S=#set{sources=Sources, rid=RID, next_req=Req, pending=none}) ->
    {ok, Prep, _} = prepare_op(S),
    RequestID = {RID, Req},
    sync_select(S, [{RequestID, Source, Prep} || Source <- Sources]);
    %%do_sync(S#set{pending={RequestID,Source,Prep}});
sync(S=#set{pending=Pending}) when is_tuple(Pending) ->
    do_sync(S).

sync_select(#set{}, []) ->
    {error, no_replica};
sync_select(S=#set{}, [Pending|PS]) ->
    case do_sync(S#set{pending=Pending}) of
        switch ->
            sync_select(S, PS);
        Res ->
            Res
    end.

do_sync(S=#set{cid=CID, pending={RequestID={_RID,ReqN},Source,Prep},
               mod=CMod, crdt=CRDT}) ->
    case crdt_service:passive_op(Source, CID, RequestID, Prep) of
        {ok, ResC, ResMAC} ->
            %% XXX: merging invalidates the MAC, of course.
            %%
            %% Either keep a pristine one, or have the server ensure
            %% its state is a true causal descendant of ours.
            %%
            %% Or compute the delta (should be all removed) and send
            %% an appropriate remove operation, until the merge returns
            %% an identical CRDT.
            Merged = CMod:merge(CRDT, CMod:from_binary(ResC)),
            {ok, S#set{next_req=ReqN+1, crdt=Merged, mac=ResMAC,
                       adds=[], rems=[], pending=none}};
        {error, notfound} ->
            switch;
        {error, {precondition, Cond}} ->
            io:format("Precondition failed at source ~p: ~p~n",
                      [Source, Cond]),
            switch;
        {error, _Reason} ->
            {retry, S}
    end.



prepare_op(#set{adds=[], rems=[]}) ->
    {error, noop};
prepare_op(#set{mod=CMod, crdt=CRDT, adds=[AddE], rems=[]}) ->
    CMod:prepare({passive_add, AddE}, nil, CRDT);
prepare_op(#set{mod=CMod, crdt=CRDT, adds=Adds, rems=[]}) ->
    CMod:prepare({passive_add_all, Adds}, nil, CRDT);
prepare_op(#set{mod=CMod, crdt=CRDT, adds=[], rems=[RemE]}) ->
    CMod:prepare({passive_remove, RemE}, nil, CRDT);
prepare_op(#set{mod=CMod, crdt=CRDT, adds=[], rems=Rems}) ->
    CMod:prepare({passive_remove_all, Rems}, nil, CRDT);
prepare_op(S=#set{adds=Adds, rems=Rems})
  when Adds /= [] andalso Rems /= [] ->
    {ok, AddOp, _} = prepare_op(S#set{rems=[]}),
    {ok, RemOp, _} = prepare_op(S#set{adds=[]}),
    {ok, {update, [AddOp, RemOp]}, S}.


randomize_nodes(Nodes) ->
    S = lists:keysort(1, [{random:uniform(), Node} || Node <- Nodes]),
    [{crdt_service,Node} || {_, Node} <- S].

-ifdef(TEST).

local_ops_test() ->
    {ok, S} = logged_orswot:update({add_all, [a, b, c]},
                                   actor,
                                   logged_orswot:new()),
    ?assertEqual([a,b,c], logged_orswot:value(S)),
    PS1 = new(logged_orswot, S, <<"anteater">>, <<"C0256">>),
    ?assertEqual([a,b,c], lists:sort(value(PS1))),
    PS2 = update({add, d}, PS1),
    ?assertEqual([a,b,c,d], lists:sort(value(PS2))),
    PS3 = update({remove, b}, PS2),
    ?assertEqual([a,c,d], lists:sort(value(PS3))),
    PS4 = update({add, b}, PS3),
    ?assertEqual([a,b,c,d], lists:sort(value(PS4))),
    #set{adds=Adds, rems=Rems} = PS4,
    ?assert(ordsets:is_element(b, Adds)),
    ?assertNot(ordsets:is_element(b, Rems)),
    ?assert(ordsets:is_element(d, Adds)),
    #set{adds=Adds5, rems=Rems5} = update({remove, d}, PS4),
    ?assertNot(ordsets:is_element(d, Adds5)),
    ?assertNot(ordsets:is_element(d, Rems5)).

-endif.
