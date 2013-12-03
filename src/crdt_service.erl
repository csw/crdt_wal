-module(crdt_service).

-behaviour(gen_server).

%% API
-export([start_link/0, create/3, fetch/2,
         find_recovery/1, finish_recovery/0,
         send_passive_fun/2, passive_op/4, acknowledge/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type key()        :: crdt_server:crdt_id().
-type service()    :: module() | pid().
-type replica_id() :: term().
-type request_id() :: {replica_id(), non_neg_integer()}.
-type crdt_op()    :: term().
-type crdt_state() :: binary().
-type crdt_mac()   :: binary().
-type crdt_state_reply() :: {'ok', crdt_state(), crdt_mac()}.


-record(state, {actor         :: riak_dt:actor(),
                s_table       :: ets:tid(),
                mode=recovery :: 'recovery' | 'normal'
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

find_recovery(Key) ->
    gen_server:call(?SERVER, {find_recovery, bin_key(Key)}).

-spec finish_recovery() -> 'ok'.
finish_recovery() ->
    gen_server:call(?SERVER, finish_recovery).

-spec fetch(service(), key()) -> crdt_state_reply().
fetch(Service, Key) ->
    gen_server:call(Service, {fetch, bin_key(Key)}).

-spec create(service(), key(), module()) -> crdt_state_reply().
create(Service, Key, Mod) when is_atom(Mod) ->
    gen_server:call(Service, {create, bin_key(Key), Mod}).

-spec passive_op(service(), key(), request_id(), crdt_op()) ->
                        crdt_state_reply().
passive_op(Service, Key, RequestID, Op) ->
    gen_server:call(Service, {passive_op, bin_key(Key), RequestID, Op}).

send_passive_fun(Service, Key) ->
    fun(RequestID, Prepared) ->
            passive_op(Service, Key, RequestID, Prepared)
    end.

acknowledge(Service, RequestID) ->
    gen_server:call(Service, {acknowledge, RequestID}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Actor} = application:get_env(actor),
    ServerT = ets:new(crdt_servers, []),
    ok = requests:init(),
    {ok, #state{actor=Actor, s_table=ServerT}}.

handle_call({passive_op, Key, RequestID, Op}, From, S=#state{mode=normal}) ->
    {ok, Pid} = fetch_server(Key, normal, S),
    ok = crdt_server:forward(Pid, {passive_op, RequestID, Op}, From),
    {noreply, S};

handle_call({create, Key, Mod}, From, S=#state{mode=normal}) ->
    {ok, Pid} = fetch_server(Key, {new, Mod}, S),
    ok = crdt_server:forward(Pid, fetch, From),
    {noreply, S};

handle_call({fetch, Key}, From, S=#state{mode=normal}) ->
    {ok, Pid} = fetch_server(Key, normal, S),
    ok = crdt_server:forward(Pid, fetch, From),
    {noreply, S};

handle_call({acknowledge, Request}, _From, S=#state{mode=normal}) ->
    ok = requests:track(Request, acknowledged),
    {reply, ok, S};

handle_call({find_recovery, Key}, _From, S=#state{mode=recovery}) ->
    {ok, Pid} = fetch_server(Key, recovery, S),
    {reply, {ok, Pid}, S};

handle_call(finish_recovery, _From, S=#state{mode=recovery}) ->
    ok = finish_recovery(S),
    io:format("Recovery finished, CRDT service entering normal mode.~n"),
    {reply, ok, S#state{mode=normal}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fetch_server(Key, Mode, S=#state{s_table=ServerT}) ->
    case ets:lookup(ServerT, Key) of
        []    -> start_server(Key, Mode, S);
        [{Key,Pid}] -> {ok, Pid}
    end.

start_server(Key, Mode, #state{actor=Actor, s_table=ServerT}) ->
    io:format("Starting CRDT server in mode ~w, key=~w~n",
              [Mode, Key]),
    {ok, Pid} = crdt_server_sup:add_server(Actor, Key, Mode),
    %% TODO: monitor?
    true = ets:insert(ServerT, {Key, Pid}),
    {ok, Pid}.
    
finish_recovery(#state{s_table=ServerT}) ->
    _Total =
        ets:foldl(fun({_Key, Pid}, N) ->
                          crdt_server:finish_recovery(Pid),
                          N+1
                  end,
                  0,
                  ServerT),
    ok.

bin_key(Key) when is_binary(Key) ->
    Key;
bin_key(Key) when is_list(Key) ->
    list_to_binary(Key).
