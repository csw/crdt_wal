%% passive_set.erl: client interface for passive Set CRDTs.

-module(passive_set).

-export_type([set/1]).

-export([new/4, value/1, update/2, sync/2]).

-type smod()         :: module().
-type replica_id()   :: term().
-type counter()      :: non_neg_integer().
-type mac()          :: term().

-type member()     :: term().
-type set_op()     :: {'add', member()} | {'remove', member()}.

-record(set, {mod                  :: module(),
              crdt                 :: term(),
              cid                  :: term(),
              rid                  :: replica_id(),
              next_req=0           :: counter(),
              mac=none             :: mac(),
              adds=[]              :: ordsets:ordset(member()),
              rems=[]              :: ordsets:ordset(member()),
              pending=none         :: counter() | 'none',
              pending_replica=none :: term() | 'none'
             }).
-type set(CRDT)                    :: #set{crdt :: CRDT}.

-spec new(smod(), CRDT, term(), replica_id()) -> set(CRDT).
new(Mod, CRDT, CID, RID) ->
    #set{mod=Mod, crdt=CRDT, cid=CID, rid=RID}.

-spec value(set(_CRDT)) -> [member()].
value(#set{mod=Mod, crdt=CRDT, adds=Adds, rems=Rems}) ->
    Orig = Mod:value(CRDT),
    ordsets:union(ordsets:subtract(Orig, Rems), Adds).

-spec update(set_op(), set(CRDT)) -> set(CRDT).
update({add, E}, S=#set{adds=Adds, rems=Rems}) ->
    S#set{adds=ordsets:add_element(E, Adds),
          rems=ordsets:del_element(E, Rems)};
update({remove, E}, S=#set{mod=CMod, adds=Adds, rems=Rems}) ->
    case CMod:value({contains, E}) of
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

-spec sync(set(CRDT), term()) -> {'ok', set(CRDT)} |
                                 {'retry', set(CRDT)}.
sync(S=#set{adds=[], rems=[]}, _Service) ->
    {ok, S};
sync(S=#set{adds=Adds, rems=Rems}, Service) ->
    Ops = ([ {passive_remove, E} || E <- Rems ]
           ++ [ {passive_add, E} || E <- Adds ]),
    S3 = lists:foldl(fun(Op, S1) ->
                             {ok, S2} = sync_op(Op, S1, Service),
                             S2
                     end,
                     S,
                     Ops),
    {ok, S3}.

sync_op(Op, S=#set{mod=CMod, crdt=CRDT, cid=CID, rid=RID, next_req=Req},
        Service) ->
    case CMod:prepare(Op, nil, CRDT) of
        {ok, Prep, CRDT1} when CRDT == CRDT1 ->
            RequestID = {RID, Req},
            {ok, ResC, ResMAC} =
                crdt_service:passive_op(Service, CID, RequestID, Prep),
            %% XXX: merging invalidates the MAC, of course
            Merged = CMod:merge(CRDT, ResC),
            {ok, S#set{next_req=Req+1, crdt=Merged, mac=ResMAC}}
    end.

