-module(crdt_server).

-behaviour(gen_server).

%% API
-export([start_link/3, forward/3]).
-export([recover/3, finish_recovery/1, is_tracked_request/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export_type([crdt_id/0, crdt_bin/0, mac_key/0, mac/0,
              crdt_log_rec/0]).

-define(SERVER, ?MODULE).

-define(MAC_KEY_LEN, 20). % SHA-1

-type crdt_id()    :: binary().
-type crdt_op()    :: term().
-type crdt_bin()   :: binary().
-type mac_key()    :: binary().
-type mac()        :: binary().

-record(state, {cid         :: crdt_id(),
                mode=normal :: 'normal' | 'recovery',
                mod         :: module(),
                actor       :: riak_dt:actor(),
                mac_key     :: mac_key(),
                crdt=none   :: riak_dt:crdt() | 'none',
                lsn=none    :: wal:lsn() | 'none'
               }).

-type crdt_log_rec() :: {'passive_op',
                         crdt_id(),
                         crdt_service:request_id(),
                         crdt_op()}.

%%%===================================================================
%%% API
%%%===================================================================

start_link(Actor, Key, Mode) ->
    gen_server:start_link(?MODULE,
                          {Actor, Key, Mode},
                          []).


recover(Pid, LSN, Op) ->
    gen_server:call(Pid, {recover, LSN, Op}).

finish_recovery(Pid) ->
    gen_server:call(Pid, finish_recovery).

forward(Pid, Request, From) ->
    io:format("Forwarding request to ~p: ~p~n", [Pid, Request]),
    gen_server:cast(Pid, {Request, From}),
    ok.

-spec is_tracked_request(crdt_log_rec()) -> boolean().
is_tracked_request(_) ->
    true.

%% start_existing(Mod, CID) ->
%%     gen_server:start_link({local, ?SERVER}, ?MODULE, {existing, Mod, CID}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Actor, CID, {new, Mod}}) ->
    Key = crypto:strong_rand_bytes(?MAC_KEY_LEN),
    gen_server:cast(self(), sync_write),
    {ok, #state{cid=CID, mod=Mod, actor=Actor, mac_key=Key,
                crdt=Mod:new()}};

init({Actor, CID, recovery}) ->
    {ok, State} = load_state(Actor, CID),
    {ok, State#state{mode=recovery}};

init({Actor, CID, normal}) ->
    {ok, State} = load_state(Actor, CID),
    {ok, State#state{mode=normal}}.


%% init({existing, Mod, CID}) ->
%%     {ok, #state{}}.

handle_call({passive_op, RequestID, Op}, _From, S0=#state{mode=normal}) ->
    {ok, S} = do_passive(RequestID, Op, S0),
    {reply, ret_crdt(S), S};

handle_call(fetch, _From, S=#state{mode=normal}) ->
    {reply, ret_crdt(S), S};

handle_call({recover, LSN, Rec}, _From, S0=#state{mode=recovery}) ->
    {ok, S} = apply_record(Rec, LSN, S0),
    {reply, ok, S};

handle_call(finish_recovery, _From, S0=#state{mode=recovery}) ->
    {reply, ok, S0#state{mode=normal}}.



handle_cast({{passive_op, RequestID, Op}, From}, S0=#state{mode=normal}) ->
    {ok, S} = do_passive(RequestID, Op, S0),
    Reply = ret_crdt(S),
    gen_server:reply(From, Reply),
    {noreply, S};

handle_cast({fetch, From}, S=#state{mode=normal}) ->
    Reply = ret_crdt(S),
    gen_server:reply(From, Reply),
    {noreply, S};

handle_cast({{recover, LSN, Rec}, From}, S0=#state{mode=recovery}) ->
    {ok, S} = apply_record(Rec, LSN, S0),
    gen_server:reply(From, ok),
    {noreply, S};

handle_cast(sync_write, S=#state{}) ->
    ok = storage:store_crdt_sync(stored(S)),
    {noreply, S}.


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec load_state(riak_dt:actor(), crdt_id()) -> {'ok', #state{}}.
load_state(Actor, CID) ->
    {ok, {CID, Mod, MACKey, CBin, LSN}} = storage:load_crdt(CID),
    CRDT = Mod:from_binary(CBin),
    {ok, #state{cid=CID, mod=Mod, actor=Actor, mac_key=MACKey,
                crdt=CRDT, lsn=LSN}}.

-spec do_passive(crdt_service:request_id(), crdt_service:crdt_op(),
                 #state{}) ->
                        {'ok', #state{}}.
do_passive(RequestID, Op,
           S=#state{cid=CID, mod=Mod, actor=Actor, crdt=CRDT}) ->
    case requests:check(RequestID) of
        unknown ->
            case Mod:prepare(Op, Actor, CRDT) of
                {ok, Prep, _CPrep} ->
                    io:format("Prepared op: ~p~n", [Prep]),
                    commit_record({passive_op, CID, RequestID, Prep}, S)
            end;
        State ->
            %% Duplicate request, do nothing
            io:format("Ignoring duplicate request ~p, state=~p.~n",
                      [RequestID, State]),
            {ok, S}
    end.

-spec commit_record(crdt_log_rec(), #state{}) -> {'ok', #state{}}.
commit_record(Record, S=#state{cid=CID}) ->
    {ok, LSN} = wal_mgr:log_durable(CID, Record),
    apply_record(Record, LSN, S).

-spec apply_record(crdt_log_rec(), wal:lsn(), #state{}) -> {'ok', #state{}}.
apply_record({passive_op, CID, RequestID, Prep}=Op,
             OpLSN,
             S=#state{mod=Mod, actor=Actor, crdt=CRDT, lsn=LSN})
  when LSN == none orelse OpLSN > LSN ->
    io:format("Applying log record ~16.16.0B to CRDT ~p:~n~p~n",
             [OpLSN, CID, Op]),
    {ok, CEff} = Mod:effect(Prep, Actor, CRDT),
    S1 = S#state{crdt=CEff, lsn=OpLSN},
    ok = requests:committed(RequestID, OpLSN),
    ok = storage:store_crdt(stored(S1)),
    ok = wal_mgr:clean_record(OpLSN, data),
    {ok, S1};

apply_record({passive_op, _, RequestID, _}, OpLSN, S=#state{lsn=LSN}) ->
    ok = requests:committed(RequestID, OpLSN),
    ok = wal_mgr:clean_record(OpLSN, data),
    io:format("Skipping log record ~16.16.0B, already at ~16.16.0B.~n",
              [OpLSN, LSN]),
    {ok, S};

apply_record(_Op, _OpLSN, S=#state{}) ->
    %% before the current LSN, skip it
    {ok, S}.

stored(#state{cid=CID, mod=Mod, mac_key=Key, crdt=CRDT, lsn=LSN}) ->
    {CID, Mod, Key, Mod:to_binary(CRDT), LSN}.

-spec ret_crdt(#state{}) -> {'ok', crdt_bin(), mac()}.
ret_crdt(S=#state{mod=Mod, crdt=CRDT, mac_key=Key}) ->
    io:format("Returning CRDT from state:~n~p~n", [S]),
    CBin = Mod:to_binary(CRDT),
    MAC = crypto:sha_mac(Key, CBin),
    {ok, CBin, MAC}.
